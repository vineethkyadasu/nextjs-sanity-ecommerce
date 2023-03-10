import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import bigquery
from datetime import timedelta
import json

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
        beam.window.FixedWindows(size=timedelta(seconds=180)),
        timestamp_combiner=beam.transforms.trigger.TimestampCombiner.OUTPUT_AT_END)
    # Group the events by item ID and count the number of events
    | 'Group by item ID' >> beam.GroupByKey()
    | 'Count by item ID' >> beam.Map(lambda x: (x[0], len(x[1])))
    # Filter the items that have a count of 5 or more
    | 'Filter by count' >> beam.Filter(lambda x: x[1] >= 5)
    | 'Write to BigQuery' >> bigquery.WriteToBigQuery(
        table='eventData',
        dataset='gtm_monitoring',
        project='gtm-t8hbgcc-owqxz',
        schema={
    'fields': [
        {'name': 'item_name', 'type': 'STRING'},
        {'name': 'count', 'type': 'INTEGER'},
        {'name': 'timestamp', 'type': 'TIMESTAMP'}
    ]
},
        create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND))
