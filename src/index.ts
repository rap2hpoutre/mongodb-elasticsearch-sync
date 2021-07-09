import { Command, flags } from "@oclif/command";
import { MongoClient } from "mongodb";
const parseSchema = require("mongodb-schema");
const elasticsearch = require("elasticsearch");
const pluralize = require("pluralize");

function parseSchemaPromise(schema: any): Promise<any> {
  return new Promise((resolve, reject) => {
    parseSchema(schema, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}

class MongoElasticSync extends Command {
  static description = "Sync MongoDB documents to Elasticsearch";

  static flags = {
    // add --version flag to show CLI version
    version: flags.version({ char: "v" }),
    help: flags.help({ char: "h" }),
    mongoUri: flags.string({ char: "u", description: "mongodb source" }),
    elasticsearchUri: flags.string({
      char: "e",
      description: "elasticsearch destination",
    }),
    singularizeName: flags.boolean({
      char: "s",
      description: "singularize document names in Elasticsearch",
    }),
  };

  static args = [{ name: "file" }];

  async run() {
    const { flags } = this.parse(MongoElasticSync);

    if (!flags.mongoUri) {
      this.error("You must specify a mongoUri");
    }
    if (!flags.elasticsearchUri) {
      this.error("You must specify an elasticsearchUri");
    }

    // List all mongodb collections
    this.log("Connecting to source…");
    const client = new MongoClient(flags.mongoUri, {
      useUnifiedTopology: true,
    });
    await client.connect();
    const db = client.db();

    this.log("Getting collections…");
    const collections = await db.listCollections().toArray();

    const collectionsSchemas = [];
    // Get all the schemas for each collection
    for (const collection of collections) {
      this.log(` Getting schema for ${collection.name}…`);
      const schema = await parseSchemaPromise(
        db.collection(collection.name).find()
      );
      collectionsSchemas.push(this.getCollectionSchema(collection, schema));
    }

    this.log("Convert to schema to elasticsearch mapping…");

    // Convert schema to elasticsearch mapping
    const mappings = collectionsSchemas.map((collectionSchema) => {
      return this.getMapping(collectionSchema, flags.singularizeName);
    });

    // Create elasticsearch index
    this.log("Creating elasticsearch index…");
    const es = new elasticsearch.Client({
      node: flags.elasticsearchUri,
    });
    for (const mapping of mappings) {
      this.log(` Creating mapping for ${mapping.index}`);
      const exists = await es.indices.exists({ index: mapping.index });
      if (exists) await es.indices.delete({ index: mapping.index });
      await es.indices.create({ index: mapping.index });
      await es.indices.putMapping(mapping);
    }

    // Sync mongodb collections to elasticsearch
    this.log("Syncing mongodb collections to elasticsearch…");
    for (const collectionSchema of collectionsSchemas) {
      this.log(` Syncing ${collectionSchema.name}`);
      // Display estimated documents count
      const estimatedDocsCount = await db
        .collection(collectionSchema.name)
        .estimatedDocumentCount();
      this.log(` Estimated documents count: ${estimatedDocsCount}`);
      const collectionData = await db
        .collection(collectionSchema.name)
        .find()
        .toArray();

      const bulk = [];
      for (const document of collectionData) {
        const { _id, __v, ...data } = document;
        bulk.push({
          index: {
            _index: flags.singularizeName
              ? pluralize.singular(collectionSchema.name)
              : collectionSchema.name,
            _type: "_doc",
            _id: _id.toString(),
          },
        });
        bulk.push({ ...this.serialize(data) });
      }
      if (bulk.length) {
        const res = await es.bulk({
          refresh: true,
          body: bulk,
        });
        if (res.error) {
          this.error(`Error syncing ${collectionSchema.name}: ${res.error}`);
        }
      }
    }
    await client.close();
  }

  getMapping(collectionSchema: any, singularize: boolean): any {
    const { name, fields } = collectionSchema;
    const exclude = ["id", "__v", "_id"];

    const mapping = {};
    for (const field of fields
      .filter((f) => f)
      .filter((f) => !exclude.includes(f.name))) {
      if (field.type === "String") {
        mapping[field.name] = {
          type: "text",
          fields: { keyword: { type: "keyword", ignore_above: 256 } },
        };
      } else if (field.type === "Number") {
        mapping[field.name] = { type: "double" };
      } else if (field.type === "Boolean") {
        mapping[field.name] = { type: "boolean" };
      } else if (field.type === "Date") {
        mapping[field.name] = { type: "date" };
      } else if (field.type === "GeoPoint") {
        mapping[field.name] = { type: "geo_point" };
      }
    }
    return {
      index: singularize ? pluralize.singular(name) : name,
      type: "_doc",
      include_type_name: true,
      body: {
        _doc: {
          properties: mapping,
        },
      },
    };
  }

  getCollectionSchema(collection: any, schema: any): any {
    return {
      name: collection.name,
      fields: schema.fields.map((s) => {
        const type = this.findBestProbabilityType(s.types);
        if (type.name === "Array") {
          return {
            name: s.name,
            type: this.findBestProbabilityType(type.types).name,
            isArray: true,
          };
        } else if (type.name === "Document") {
          if (
            type.fields.find((f) => f.name === "lat") &&
            type.fields.find((f) => f.name === "lon") &&
            type.fields.length === 2
          ) {
            return {
              name: s.name,
              type: "GeoPoint",
              isArray: false,
            };
          } else {
            // We currently ignore nested documents.
            return null;
          }
        } else {
          return {
            name: s.name,
            type: type.name,
          };
        }
      }),
    };
  }

  findBestProbabilityType(types: any[]): any {
    // We assume undefined types are the same as string.
    if (types.filter((t) => t.name !== "Undefined").length === 0) {
      return { name: "String" };
    }
    const max = types
      .filter((t) => t.name !== "Undefined")
      .reduce((max, type) => {
        return type.probability > max.probability ? type : max;
      });
    return max;
  }

  serialize(data: any): any {
    const result = {};
    for (const key of Object.keys(data)) {
      const value = data[key];
      if (value instanceof Date) {
        result[key] = value.toISOString();
      } else if (Array.isArray(value)) {
        result[key] = value;
      } else if (value instanceof Object) {
        result[key] = this.serialize(value);
      } else {
        result[key] = value;
      }
    }
    return result;
  }
}

export = MongoElasticSync;
