mongodb-elasticsearch-sync
==========================

Sync MongoDB and Elasticseach database: copy (and convert) all data from a MongoDB database
and index it in Elasticsearch.

[![oclif](https://img.shields.io/badge/cli-oclif-brightgreen.svg)](https://oclif.io)
[![Version](https://img.shields.io/npm/v/mongodb-elasticsearch-sync.svg)](https://npmjs.org/package/mongodb-elasticsearch-sync)
[![License](https://img.shields.io/npm/l/mongodb-elasticsearch-sync.svg)](https://github.com/rap2hpoutre/mongodb-elasticsearch-sync/blob/main/package.json)

## Usage

Run this command by giving a MongoDB URI and Elasticsearch URI (no need to install first thanks to `npx`):

```bash
npx mongodb-elasticsearch-sync \
  --mongodbUri=mongodb://localhost:27017/source \
  --elasticsearchUri=mongodb://localhost:27017/anonymized
```

☝️ Be careful, since Elasticsearch indexed will be reset.

### Options

Use `--singularizeName` to transform `users` collection in MongoDB into `user` in Elasticsearch.

```bash
npx mongodb-elasticsearch-sync \
  --mongodbUri=mongodb://localhost:27017/source \
  --elasticsearchUri=mongodb://localhost:27017/anonymized
  --singularizeName
```
## Why

I recently created a (tool)[https://github.com/rap2hpoutre/mongodb-anonymizer#mongodb-anonymizer] 
to anonymize MongoDB database (from a source db to a target db) to help me build a staging 
environment for a project. Since I use MongoDB with Elasticsearch for this project, I needed a
one-line command to create the Elascticsearch indices after this process.
