import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import bigquery
from datetime import timedelta
import json

def format_output(item_count):
    item_name, count, timestamp = item_count
    return {
        'item_name': item_name,
        'count': count,
        'timestamp': timestamp
    }

# Set up the pipeline options
options = PipelineOptions.from_dictionary({
    'runner': 'DataflowRunner',
    'project': 'gtm-t8hbgcc-owqxz',
    'staging_location': 'gs://stage_bkt/staging',
    'temp_location': 'gs://temp9283/temp',
    'region': 'us-central1',
    'streaming': True
})

# Set up the pipeline
with beam.Pipeline(options=options) as p:
    (p
    # Read the stream of shopping cart events from a Pub/Sub topic
    | 'Read from Pub/Sub' >> beam.io.gcp.pubsub.ReadFromPubSub(topic='projects/gtm-t8hbgcc-owqxz/topics/GA-Event')
    # Parse the event messages into Python dictionaries
    | 'Parse JSON' >> beam.Map(lambda x: json.loads(x))
    # Extract the item IDs and timestamps from each event
    | 'Extract item name and timestamp' >> beam.Map(lambda x: (x['item_name'], x['timestamp']))
    # Assign timestamps and window the events into 30-second intervals
    | 'Timestamp and window' >> beam.WindowInto(
        beam.window.FixedWindows(size=30),
        accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        allowed_lateness=0)
    # Group the events by item ID and count the number of events
    | 'Group by item ID' >> beam.GroupByKey()
    | 'Count items' >> beam.Map(lambda x: (x[0], len(x[1]) if x[1] else 0, x[1][0] if x[1] else None))
    | 'Filter by count' >> beam.Filter(lambda x: x[1] >= 5)
    | 'Format output' >> beam.Map(format_output)
    # Filter the items that have a count of 5 or more

    | 'Write to BigQuery' >> bigquery.WriteToBigQuery(
        table='itemInDemand',
        dataset='gtm_monitoring',
        project='gtm-t8hbgcc-owqxz',
        schema='item_name:STRING,count:INTEGER,timestamp:TIMESTAMP',
        create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND))
