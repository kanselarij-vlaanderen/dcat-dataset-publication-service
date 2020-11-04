import fs from 'fs-extra';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeString, sparqlEscapeUri } from 'mu';

import { BESLUITVORMING_CATALOG_URI, VERGADERACTIVITEIT_TYPE } from '../config';

class DataSet {

  constructor(graph) {
    this.graph = graph;

    const timestamp = Date.now();
    this.filePath = `/tmp/${timestamp}.ttl`;

    this.distributions = [];
  }

  /**
   * Prepare the dataset resources:
   * 1/ generate a ttl file with the dataset graph triples
   * 2/ generate distributions for the attachments
   * 3/ generate a distribution for the ttl file
   * 4/ generate a dataset
   */
  async prepare() {
    await this.generateTtlFile();
    await this.generateAttachmentDistributions();
    await this.generateTtlDistribution();
    await this.generateDataSet();
  }

  /**
    * Get the triples from the temporary dataset graph   *
    * @public
  */
  getTtlFilepath() {
    return this.filePath;
  }

  /**
   * Generate a ttl file with the dataset graph triples
   * @private
  */
  async generateTtlFile() {
    console.log(`Generate ttl file ${this.filePath} with dataset triples `);
    const triples = await this.getGraphTriples(this.graph);
    const statements = toStatements(triples);
    fs.writeFile(this.filePath, statements);
  }

  /**
   * Generate a distribution for the dataset attachments and 
   * insert the distribution into the graph
   * @private
   */
  async generateAttachmentDistributions() {
    console.log(`Generate distributions for the attachments`);
    // TODO is the distribution type in the query correct - should this come from config?

    // TODO is this URI correct?
    const uri = `http://kanselarij.vo.data.gift/distribution/id/`;

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX  dct:  <http://purl.org/dc/terms/>

      INSERT {
        GRAPH <${this.graph}> {
          ?uri a dcat:Distribution ;
            mu:uuid ?uuid ;
            dct:type <http://themis.vlaanderen.be/id/concept/distribution-type/c4d99dde-3df9-4da1-8136-9a3b2de82de4> ;
            dct:subject ?s . 
        }
      } WHERE {
        ?s a nfo:FileDataObject ;
            nie:dataSource ?d .
            BIND ("${mu.uuid()}" AS ?uuid)
            BIND (uri(concat("${uri}", ?uuid)) AS ?uri)
      }
  `);
  }

  /**
   * Generate a distribution for the dataset ttl file and 
   * insert the distribution into the graph
   * @private
   */
  async generateTtlDistribution() {
    console.log(`Generate distribution for the ttl file`);

    // TODO is the distribution type in the query correct - should this come from config?

    const uuid = mu.uuid();
    // TODO is this URI correct?
    const uri = `http://kanselarij.vo.data.gift/distribution/id/${uuid}`;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX  dct:  <http://purl.org/dc/terms/>

      INSERT DATA {
        GRAPH <${this.graph}> {
          <${uri}> a dcat:Distribution ;
            mu:uuid "${uuid}" ;
            dct:type <http://themis.vlaanderen.be/id/concept/distribution-type/dd5bfc23-8f88-4df5-80f6-a9f72e08d7c4> ;
            dct:subject <${this.filePath}> . 
        } 
      }
    `);
  }

  async generateDataSet() {
    console.log(`Generate dataset`);

    const uuid = mu.uuid();
    // TODO is this URI correct?
    const uri = `http://kanselarij.vo.data.gift/dataset/id/${uuid}`;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

      INSERT {
        GRAPH <${this.graph}> {
          <${uri}> a dcat:DataSet ;
            mu:uuid "${uuid}" ;
            dct:type <${VERGADERACTIVITEIT_TYPE}> ;
            dcat:catalog <${BESLUITVORMING_CATALOG_URI}> ;
            dct:subject ?activity ;
            dcat:distributions ?distribtion
        } 
      } WHERE {
          ?distribution a dcat:Distribution .
          ?activity rdf:type <${VERGADERACTIVITEIT_TYPE}> .
      }
    `);

    this.uri = uri;
  }


  /**
    * Get all the triples from the dataset graph
    * @private
  */
  async getGraphTriples() {
    const result = await query(`
            SELECT * WHERE {
              GRAPH <${this.graph}> {
                ?subject ?predicate ?object .
              }
            }
          `);

    if (result.results.bindings.length) {
      return result.results.bindings;
    } else {
      return null;
    }
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

export default DataSet;