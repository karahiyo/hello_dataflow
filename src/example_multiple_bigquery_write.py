import argparse
import logging

import apache_beam as beam


def run(argv=None):
  """Run the workflow."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--output',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=wrong-import-order, wrong-import-position

    table_schema = bigquery.TableSchema()

    # Fields that use standard types.
    kind_schema = bigquery.TableFieldSchema()
    kind_schema.name = 'kind'
    kind_schema.type = 'string'
    kind_schema.mode = 'nullable'
    table_schema.fields.append(kind_schema)

    full_name_schema = bigquery.TableFieldSchema()
    full_name_schema.name = 'fullName'
    full_name_schema.type = 'string'
    full_name_schema.mode = 'required'
    table_schema.fields.append(full_name_schema)

    age_schema = bigquery.TableFieldSchema()
    age_schema.name = 'age'
    age_schema.type = 'integer'
    age_schema.mode = 'nullable'
    table_schema.fields.append(age_schema)

    gender_schema = bigquery.TableFieldSchema()
    gender_schema.name = 'gender'
    gender_schema.type = 'string'
    gender_schema.mode = 'nullable'
    table_schema.fields.append(gender_schema)

    # A nested field
    phone_number_schema = bigquery.TableFieldSchema()
    phone_number_schema.name = 'phoneNumber'
    phone_number_schema.type = 'record'
    phone_number_schema.mode = 'nullable'

    area_code = bigquery.TableFieldSchema()
    area_code.name = 'areaCode'
    area_code.type = 'integer'
    area_code.mode = 'nullable'
    phone_number_schema.fields.append(area_code)

    number = bigquery.TableFieldSchema()
    number.name = 'number'
    number.type = 'integer'
    number.mode = 'nullable'
    phone_number_schema.fields.append(number)
    table_schema.fields.append(phone_number_schema)

    # A repeated field.
    children_schema = bigquery.TableFieldSchema()
    children_schema.name = 'children'
    children_schema.type = 'string'
    children_schema.mode = 'repeated'
    table_schema.fields.append(children_schema)

    def create_random_record(record_id):
      return {
          'kind': 'kind' + record_id,
          'fullName': 'fullName' + record_id,
          'age': int(record_id) * 10,
          'gender': 'male',
          'phoneNumber': {
              'areaCode': int(record_id) * 100,
              'number': int(record_id) * 100000
          },
          'children': [
              'child' + record_id + '1',
              'child' + record_id + '2',
              'child' + record_id + '3'
          ]
      }

    # pylint: disable=expression-not-assigned
    record_ids = p | 'CreateIDs' >> beam.Create(['1', '2', '3', '4', '5'])
    records = record_ids | 'CreateRecords' >> beam.Map(create_random_record)
    grouped_records = (
            records
            | beam.Map(lambda x: (x.get("kind"), x))
            | beam.GroupByKey())
    grouped_records | 'Huga' >> beam.Map(lambda x: beam.ParDo(CustomPrintDoFn(x)))
    grouped_records | beam.Keys() | beam.Map(lambda x: {print(x); yield x}) | beam.Values() | beam.Map(print)
    # やりたいことは
    # PTransformの中でkeyとvalueに分解して、
    # keyのデータにアクセスできる状態でvalueに対してParDoをしたい

    # -> や、keyを引数に与えつつvalue  | PTransformをしたい


class CustomPrintDoFn(beam.DoFn):
    def process(self, element):
        print("-------")
        print(element)
        print("-------")
        yield element
        

    # records | 'write' >> beam.io.WriteToBigQuery(
    #     known_args.output,
    #     schema=table_schema,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    # Run the pipeline (all operations are deferred until run() is called).

    # そもそもできるのかどうか
    # レスポンスにPCollectionを返す、はやれる。たぶん問題ないが
    # 初期化方法がわからぬ

    # Keyはside inputとしてわたす？


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
