import apache_beam as beam
import ast
import json

SUBSCRIPTION_ID = 'projects/york-cdf-start/subscriptions/pas_dataflow_topic-sub'
OUT_TOP = 'projects/york-cdf-start/topics/dataflow-order-stock-update'
USD_TABLE = 'york-cdf-start:pas_df.usd_order_payment_history'
EUR_TABLE = 'york-cdf-start:pas_df.eur_order_payment_history'
GBP_TABLE = 'york-cdf-start:pas_df.gbp_order_payment_history'
currency = ['USD', 'EUR', 'GBP']

TABLE_SCHEMA = {
    'fields': [{
        'name': 'order_id', 'type': 'STRING', 'mode': 'REQUIRED'
    },{
        'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED', 'fields':[{
            'name': 'order_building_number', 'type': 'STRING', 'mode': 'NULLABLE'
        },{
            'name': 'order_street_name', 'type': 'STRING', 'mode': 'NULLABLE'
        },{
            'name': 'order_city', 'type': 'STRING', 'mode': 'NULLABLE'
        },{
            'name': 'order_state_code', 'type': 'STRING', 'mode': 'NULLABLE'
        },{
            'name': 'order_zip_code', 'type': 'STRING', 'mode': 'NULLABLE'
        }],
    },{
        'name': 'customer_first_name', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name': 'customer_last_name', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name': 'customer_ip', 'type': 'STRING', 'mode': 'NULLABLE'
    },{
        'name': 'cost_total', 'type': 'FLOAT', 'mode': 'NULLABLE'
    }]
}

class Transform(beam.DoFn):
    new_dict = {}
    def process(self, element):
        # calculate cost total and collect item ids
        all_items = element["order_items"]
        price = 0
        i = 0
        while i < len(all_items):
            price += float(all_items[i]["price"])
            i += 1
        cost_total = sum([price, float(element["cost_shipping"]), float(element["cost_tax"])])

        #convert needed values
        customer_first_name = element["customer_name"].split()[0]
        customer_last_name = element["customer_name"].split()[1]
        order_building_number = element["order_address"].split()[0]
        street_name_array = (element["order_address"].split(",")[0]).split()[1:]
        order_street_name = " ".join(street_name_array)
        order_city = element["order_address"].split(",")[1]
        order_state_code = (element["order_address"].split(",")[-1]).split()[-2]
        order_zip_code = element["order_address"].split()[-1]

        #compile needed values into new dictionary
        new_dict = {
        "order_id" : element["order_id"],
        "order_address":[
            {"order_building_number" : order_building_number,
             "order_street_name" : order_street_name,
             "order_city" : order_city,
             "order_state_code" : order_state_code,
             "order_zip_code" : order_zip_code}],
        "customer_first_name" : customer_first_name,
        "customer_last_name" : customer_last_name,
        "customer_ip": element["customer_ip"],
        "cost_total" : round(cost_total, 2)
        }
        yield new_dict

def by_currency(element, num_currency):
    return currency.index(element['order_currency'])

class Create_Message(beam.DoFn):
    message = {}
    def process(self, element):
        all_items = element["order_items"]
        item_list = []
        i = 0
        while i < len(all_items):
            item_list.append(all_items[i]["id"])
            i += 1
        message = {
            "order_id": element["order_id"],
            "item_list": item_list
            }
        yield message

class Conversion(beam.DoFn):
    def process(self, element):
        element=element.decode()
        element=ast.literal_eval(element)
        return [element]

def run():
    o = beam.options.pipeline_options.PipelineOptions(streaming = True, save_main_session = True)
    with beam.Pipeline(options=o) as p:
            #Read in message and convert to dictionary
        raw_data = p | 'Read in Message' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
        conv_data = raw_data | 'Convert from Bytestring to Dict' >> beam.ParDo(Conversion())

            #Create message and write to PubSub
        ps_message = conv_data | 'Create PubSub Output' >> beam.ParDo(Create_Message())
        conv_message = ps_message | 'Convert Message From Dict to Bytestring' >> beam.Map(lambda s: json.dumps(s).encode("utf-8"))
        conv_message | 'Publish USD Order' >> beam.io.WriteToPubSub(topic=OUT_TOP)

            #Partition data on currency type
        usd, eur, gbp = (
            conv_data | 'Partition' >> beam.Partition(by_currency, len(currency))
        )
            #Clean data and upload to BigQuery
        tusd = usd | 'Transform USD' >> beam.ParDo(Transform())
        teur = eur | 'Transform EUR' >> beam.ParDo(Transform())
        tgbp = gbp | 'Transform GBP' >> beam.ParDo(Transform())

        tusd | 'Print to USD Table' >> beam.io.WriteToBigQuery(
            USD_TABLE,
            schema=TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        teur | 'Print to EUR Table' >> beam.io.WriteToBigQuery(
            EUR_TABLE,
            schema=TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        tgbp | 'Print to GBP Table' >> beam.io.WriteToBigQuery(
            GBP_TABLE,
            schema=TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

if __name__ == "__main__":
    run()