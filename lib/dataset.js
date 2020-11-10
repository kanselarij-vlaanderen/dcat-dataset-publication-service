import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime, sparqlEscapeInt } from 'mu';
import fs from 'fs-extra';
import path from 'path';

import {
  BESLUITVORMING_CATALOG_URI,
  VERGADERACTIVITEIT_TYPE,
  DATASET_TYPE,
  DATASET_NEWSITEM_TYPE,
  DATASET_ATTACHMENT_TYPE,
  UPDATE_BATCH_SIZE,
  SELECT_BATCH_SIZE,
  PUBLIC_GRAPH,
  APPLICATION_DOMAIN
} from '../config';

class Dataset {

  constructor(graph) {
    this.graph = graph;

    this.filePath = `/share/${mu.uuid()}.ttl`;
  }

  /**
   * Prepare the dataset resources:
   * 1/ insert a dataset
   * 2/ generate a ttl file with the dataset graph triples
   * 3/ insert distributions for the attachments
   * 4/ insert a distribution for the ttl file
   * 
   * @public
   */
  async prepare() {
    await this.generateDataset();
    await this.generateTtlFile();
    await this.generateAttachmentDistributions();
    await this.generateTtlDistribution();
  }

  /**
   * Release the dataset and clear the graph
   */
  async release() {
    const triples = await this.getGraphTriples();
    await insertTriplesInPublicGraph(triples);
    await this.removeGraph();
  }

  /**
   * Create the new dataset
   * 
   * @private
   */
  async generateDataset() {
    const uuid = mu.uuid();
    const uri = `http://themis.vlaanderen.be/id/dataset/${uuid}`;
    const now = Date.now();

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

      INSERT {
        GRAPH <${this.graph}> {
          <${uri}> a dcat:Dataset ;
            mu:uuid "${uuid}" ;
            dct:type <${DATASET_TYPE}> ;
            dcat:catalog <${BESLUITVORMING_CATALOG_URI}> ;
            dct:subject ?meeting ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:issued ${sparqlEscapeDateTime(now)} ;
            dct:title ?title .
        } 
      } WHERE {
        GRAPH <${this.graph}> {
          ?meeting a <${VERGADERACTIVITEIT_TYPE}> ;
            besluit:geplandeStart ?start .
          BIND (CONCAT("Vergaderactiviteit in kort bestek ", ?start) AS ?title)
        }
      }
    `);

    this.datasetUri = uri;

    console.log(`Generated dataset ${this.datasetUri}`);
  }

  /**
   * Generate a ttl file containing all the dataset graph triples
   * 
   * @private
  */
  async generateTtlFile() {
    console.log(`Generate ttl file ${this.filePath} with dataset triples`);
    const triples = await this.getGraphTriples(this.graph);
    const statements = toStatements(triples);
    await fs.writeFile(this.filePath, statements);
  }

  /**
   * Insert a distribution for the dataset attachments
   * 
   * @private
   */
  async generateAttachmentDistributions() {
    console.log(`Generate distributions for the attachments`);

    const uri = `http://themis.vlaanderen.be/id/distribution/`;
    const now = Date.now();

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct:  <http://purl.org/dc/terms/>

      INSERT {
        GRAPH <${this.graph}> {
          ?uri a dcat:Distribution ;
            mu:uuid ?distributionUuid ;
            dct:type <${DATASET_ATTACHMENT_TYPE}> ;
            dct:subject ?s ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:issued ${sparqlEscapeDateTime(now)} ;
            dcat:downloadURL ?downloadURL ;
            dcat:byteSize ?byteSize ;
            dct:format ?format ;
            dct:title ?title . 
          <${this.datasetUri}> dcat:distribution ?uri.   
        }
      } WHERE {
          GRAPH <${this.graph}> {
            ?s a nfo:FileDataObject ;
              nie:dataSource ?logicalFile .
            ?logicalFile mu:uuid ?logicalFileUuid .
            OPTIONAL { ?logicalFile dct:format ?format }
            OPTIONAL { ?logicalFile nfo:fileSize ?byteSize }
            OPTIONAL { ?logicalFile nfo:fileName ?title } 
            BIND (SHA256(CONCAT(STR(?s), STR(RAND()), STRUUID())) as ?distributionUuid)          
            BIND (IRI(CONCAT("${uri}", ?distributionUuid)) AS ?uri)
            BIND (IRI(CONCAT("${APPLICATION_DOMAIN}/files/", ?logicalFileUuid, "/download" )) as ?downloadURL)
          }
      }
  `);
  }

  /**
   * Create a FileDataObject for the ttl file,
   * insert a distribution for the dataset ttl file
   * 
   * @private
   */
  async generateTtlDistribution() {
    const now = Date.now();
    const fileName = path.basename(this.filePath);
    const extension = path.extname(this.filePath);
    const format = 'text/turtle';
    const fileStats = fs.statSync(this.filePath);
    const created = new Date(fileStats.birthtime);
    const size = fileStats.size;

    const logicalFileUuid = mu.uuid();
    const logicalFileUri = `http://themis.vlaanderen.be/id/file/${logicalFileUuid}`;

    const physicalFileUuid = mu.uuid();
    const physicalFileUri = this.filePath.replace('/share/', 'share://');

    const distributionUuid = mu.uuid();
    const distributionUri = `http://themis.vlaanderen.be/id/distribution/${distributionUuid}`;

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

      INSERT {
        GRAPH <${this.graph}> {
          ${sparqlEscapeUri(logicalFileUri)} a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(logicalFileUuid)} ;
            nfo:fileName ${sparqlEscapeString(fileName)} ;
            dct:format ${sparqlEscapeString(format)} ;
            nfo:fileSize ${sparqlEscapeInt(size)} ;
            dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
            dct:creator <http://themis.vlaanderen.be/id/service/dcat-dataset-publication-service> ;
            dct:created ${sparqlEscapeDateTime(created)} .
          ${sparqlEscapeUri(physicalFileUri)} a nfo:FileDataObject ;
            mu:uuid ${sparqlEscapeString(physicalFileUuid)} ;
            nfo:fileName ${sparqlEscapeString(fileName)} ;
            dct:format ${sparqlEscapeString(format)} ;
            nfo:fileSize ${sparqlEscapeInt(size)} ;
            dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
            dct:created ${sparqlEscapeDateTime(created)} ;
            nie:dataSource ${sparqlEscapeUri(logicalFileUri)} .
          <${distributionUri}> a dcat:Distribution ;
            mu:uuid "${distributionUuid}" ;
            dct:type <${DATASET_NEWSITEM_TYPE}> ;
            dct:subject ${sparqlEscapeUri(physicalFileUri)} ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:issued ${sparqlEscapeDateTime(now)} ;
            dcat:downloadURL <${APPLICATION_DOMAIN}/files/${logicalFileUuid}/download> ;
            dcat:byteSize ${sparqlEscapeInt(size)} ;
            dct:format ${sparqlEscapeString(format)} ;
            dct:title ?title . 
          <${this.datasetUri}> dcat:distribution <${distributionUri}> .
        } 
      } WHERE {
        GRAPH <${this.graph}> {
          ?meeting a <${VERGADERACTIVITEIT_TYPE}> ;
            besluit:geplandeStart ?start .
          BIND(CONCAT("Vergaderactiviteit in kort bestek ", ?start) as ?title)
        }
      }
    `);
  }

  /**
    * Get all the triples from the dataset graph
    * 
    * @private
  */
  async getGraphTriples() {
    let triples = [];
    const count = await this.countTriples();
    if (count > 0) {
      console.log(`Parsing 0/${count} triples`);
      let offset = 0;
      const query = `    
        SELECT * WHERE {
          GRAPH <${this.graph}> {
            ?subject ?predicate ?object .
          }
        }
        LIMIT ${SELECT_BATCH_SIZE} OFFSET %OFFSET
      `;

      while (offset < count) {
        const result = await this.parseBatch(query, offset);
        triples.push(...result);
        offset = offset + SELECT_BATCH_SIZE;
        console.log(`Parsed ${offset < count ? offset : count}/${count} triples`);
      }
    }

    return triples;
  }


  async parseBatch(q, offset = 0) {
    const pagedQuery = q.replace('%OFFSET', offset);
    const result = await query(pagedQuery);

    return result.results.bindings.length ? result.results.bindings : null;
  }

  /**
   * Count the triples in the dataset graph
   */
  async countTriples() {
    const queryResult = await query(`
        SELECT (COUNT(*) as ?count)
        WHERE {
          GRAPH <${this.graph}> {
            ?s ?p ?o .
          }
        }
      `);

    return parseInt(queryResult.results.bindings[0].count.value);
  }

  /**
   * Deprecate the previous dataset
   * 1/ search for any previous dataset
   * 2/ mark it as prov:revisionOf of the current dataset
   * 3/ read the ttl file of the previous dataset and remove the triples
   * 4/ remove the downloadURL of the old dataset distributions
   * 5/ update the modified:date
   * 
   * @public
   */
  async deprecatePrevious() {
    const result = await query(`
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct:  <http://purl.org/dc/terms/>
      PREFIX prov: <http://www.w3.org/ns/prov#>

      SELECT ?dataset
      WHERE {
        GRAPH <${this.graph}> {
          <${this.datasetUri}> dct:subject ?meeting .
        }
        GRAPH <${PUBLIC_GRAPH}> {
          ?dataset a dcat:Dataset ; 
            dct:subject ?meeting . 
          FILTER NOT EXISTS { ?newerVersion prov:revisionOf ?dataset . } 
        }
      }
    `);

    if (result.results.bindings.length) {
      const binding = result.results.bindings[0];
      const previousDataset = binding['dataset'].value;
      console.log(`Found previous dataset <${previousDataset}>`);

      await update(`
        PREFIX prov: <http://www.w3.org/ns/prov#>

        INSERT DATA {
          GRAPH <${this.graph}> {
            <${this.datasetUri}> prov:revisionOf <${previousDataset}> .
          }
        }
      `);

      await removeDeprecatedTriples(previousDataset);
      await deprecateDistributions(previousDataset);
    }
  }

  async removeGraph() {
    await update(`
      DELETE WHERE { GRAPH <${this.graph}> { ?s ?p ?o } }
    `);
  }
}

function toStatements(triples) {
  const escape = function (rdfTerm) {
    const { type, value, datatype, "xml:lang": lang } = rdfTerm;
    if (type == "uri") {
      return sparqlEscapeUri(value);
    } else if (type == "literal" || type == "typed-literal") {
      // We ignore xsd:string datatypes because Virtuoso doesn't treat those as default datatype
      // Eg. SELECT * WHERE { ?s mu:uuid "4983948" } will not return any value if the uuid is a typed literal
      // Since the n3 npm library used by the producer explicitely adds xsd:string on non-typed literals
      // we ignore the xsd:string on ingest
      if (datatype && datatype != 'http://www.w3.org/2001/XMLSchema#string')
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype)}`;
      else if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    } else
      console.log(`Don't know how to escape type ${type}. Will escape as a string.`);
    return sparqlEscapeString(value);
  };
  return triples.map(function (t) {
    const subject = escape(t.subject);
    const predicate = escape(t.predicate);
    const object = escape(t.object);
    return `${subject} ${predicate} ${object} . `;
  }).join('');
}

async function removeDeprecatedTriples(previousDataset) {
  console.log(`Removing all triples belonging to previous dataset <${previousDataset}>`);
  const result = await query(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?physicalFileUri
    WHERE { 
      ?physicalFileUri a nfo:FileDataObject ;
        nie:dataSource ?logicalFileUri .
      <${previousDataset}> dcat:distribution ?distribution .
      ?distribution dct:type <${DATASET_NEWSITEM_TYPE}> ;
          dct:subject ?physicalFileUri . 
    }
  `);

  if (result.results.bindings.length) {
    const physicalFileUri = result.results.bindings[0];
    const ttlFile = physicalFileUri.replace('share://', '/share/');
    const ttl = fs.readFileSync(ttlFile, { encoding: 'utf-8' });
    const triples = await parseTtl(ttl);
    await deleteTriples(triples);
  }
}

async function deprecateDistributions(previousDataset) {
  console.log(`Deprecating distributions belonging to previous dataset <${previousDataset}>`);
  await update(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>

    DELETE {
      ?distribution dcat:downloadURL ?downloadURL ;
        dct:modified ?distributionModifiedDate .
      <${previousDataset}> dct:modified ?datasetModifiedDate
    } WHERE {
      <${previousDataset}> dcat:distribution ?distribution ;
        dct:modified ?datasetModifiedDate .
      ?distribution dcat:downloadURL ?downloadURL ;
        dct:modified ?distributionModifiedDate .
    }
  `);

  const now = Date.now();

  await update(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>
    
    INSERT {
      ?distribution dct:modified ${sparqlEscapeDateTime(now)} .
      <${previousDataset}> dct:modified ${sparqlEscapeDateTime(now)} .
    } WHERE {
      <${previousDataset}> dcat:distribution ?distribution .
    }
  `);
}

async function insertTriplesInPublicGraph(triples) {
  for (let i = 0; i < triples.length; i += UPDATE_BATCH_SIZE) {
    console.log(`Inserting triples in batch: ${i}-${i + UPDATE_BATCH_SIZE}`);
    const batch = triples.slice(i, i + UPDATE_BATCH_SIZE);
    const statements = toStatements(batch);
    await update(`
      INSERT DATA {
          GRAPH <${PUBLIC_GRAPH}> {
              ${statements}
          }
      }
    `);
  }
}

async function parseTtl(file) {
  return (new Promise((resolve, reject) => {
    const parser = new Parser();
    const triples = [];
    parser.parse(file, (error, triple) => {
      if (error) {
        reject(error);
      } else if (triple) {
        triples.push(triple);
      } else {
        resolve(triples);
      }
    });
  }));
}

async function deleteTriples(triples) {
  for (let i = 0; i < triples.length; i += UPDATE_BATCH_SIZE) {
    console.log(`Deleting triples in batch: ${i}-${i + UPDATE_BATCH_SIZE}`);
    const batch = triples.slice(i, i + UPDATE_BATCH_SIZE);
    const statements = toStatements(batch);
    await update(`
      DELETE DATA {
        GRAPH <${PUBLIC_GRAPH}> {
          ${statements}
        }
      }
    `);
  }
}

export default Dataset;