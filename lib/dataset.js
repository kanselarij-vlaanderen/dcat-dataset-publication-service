import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import mu, { sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime, sparqlEscapeInt } from 'mu';
import path from 'path';
import fs from 'fs-extra';
import { moveGraph, writeToFile } from './graph-helpers';
import { deleteTriplesFromTtl } from './ttl-helpers';

import {
  BESLUITVORMING_CATALOG_URI,
  VERGADERACTIVITEIT_TYPE,
  DATASET_TYPE,
  DATASET_NEWSITEM_TYPE,
  DATASET_ATTACHMENT_TYPE,
  PUBLIC_GRAPH,
  HOST_DOMAIN
} from '../config';

class Dataset {

  constructor(graph) {
    this.graph = graph;

    this.filePath = `/share/${mu.uuid()}.ttl`;
  }

  /**
   * Generate the DCAT dataset resources:
   * 1/ generate a TTL file with the dataset triples from the source graph
   * 2/ insert DCAT distributions for the attachments
   * 3/ insert a DCAT distribution for the TTL file
   *
   * @public
   */
  async prepare() {
    this.uuid = mu.uuid();
    this.uri = `http://themis.vlaanderen.be/id/dataset/${this.uuid}`;

    await this.generateTtlFile();
    await this.generateAttachmentDistributions();
    await this.generateTtlDistribution();
  }

  /**
   * Release the dataset as public data and update the catalog
   *
   * 1/ insert a DCAT dataset
   * 2/ add the DCAT dataset to the catalog
   * 3/ make the data from the dataset available in the public graph
   *
   * Note, the DCAT dataset must be inserted at the end once all (meta)data is available
   * because as soon as the dataset is inserted in the triplestore consumers may possibly
   * start consuming the dataset via the API.
   *
   */
  async release() {
    await this.generateDataset();
    await this.addToCatalog();
    await moveGraph(this.graph, PUBLIC_GRAPH);
  }

  /**
   * Create the new dataset
   *
   * @private
   */
  async generateDataset() {
    const now = Date.now();

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

      INSERT {
        GRAPH <${this.graph}> {
          <${this.uri}> a dcat:Dataset ;
            mu:uuid "${this.uuid}" ;
            dct:type <${DATASET_TYPE}> ;
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

    console.log(`Generated dataset ${this.uri}`);
  }

  /**
   * Generate a ttl file containing all the dataset graph triples
   *
   * @private
  */
  async generateTtlFile() {
    console.log(`Generate TTL file ${this.filePath} with dataset triples`);
    await writeToFile(this.filePath, this.graph);
  }

  /**
   * Insert a distribution for the dataset attachments
   *
   * @private
   */
  async generateAttachmentDistributions() {
    console.log('Generate distributions for the attachments');
    const now = Date.now();

    const result = await query(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct:  <http://purl.org/dc/terms/>

      SELECT * WHERE {
          GRAPH <${this.graph}> {
            ?s a nfo:FileDataObject ;
              nie:dataSource ?logicalFile .
            ?logicalFile mu:uuid ?logicalFileUuid .
            OPTIONAL { ?logicalFile dct:format ?format }
            OPTIONAL { ?logicalFile nfo:fileSize ?byteSize }
            OPTIONAL { ?logicalFile nfo:fileName ?title }
          }
      }
    `);

    for (let binding of result.results.bindings) {
      const distributionUuid = mu.uuid();
      const uri = `http://themis.vlaanderen.be/id/distribution/${distributionUuid}`;
      const downloadUrl = `${HOST_DOMAIN}/files/${binding['logicalFileUuid'].value}/download`;

      const optionalStatements = [];
      if (binding['byteSize'] && binding['byteSize'].value)
        optionalStatements.push(`<${uri}> dcat:byteSize ${sparqlEscapeInt(binding['byteSize'].value)} . `);
      if (binding['format'] && binding['format'].value)
        optionalStatements.push(`<${uri}> dct:format ${sparqlEscapeString(binding['format'].value)} . `);
      if (binding['title'] && binding['title'].value)
        optionalStatements.push(`<${uri}> dct:title ${sparqlEscapeString(binding['title'].value)} . `);

      await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>
      PREFIX dct:  <http://purl.org/dc/terms/>

      INSERT DATA {
        GRAPH <${this.graph}> {
          <${uri}> a dcat:Distribution ;
            mu:uuid ${sparqlEscapeString(distributionUuid)} ;
            dct:type <${DATASET_ATTACHMENT_TYPE}> ;
            dct:subject ${sparqlEscapeUri(binding['logicalFile'].value)} ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:issued ${sparqlEscapeDateTime(now)} ;
            dcat:downloadURL ${sparqlEscapeUri(downloadUrl)} .
            ${optionalStatements.join('\n')}
          <${this.uri}> dcat:distribution <${uri}> .
        }
      }`);
    }
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
            dct:subject ${sparqlEscapeUri(logicalFileUri)} ;
            dct:created ${sparqlEscapeDateTime(now)} ;
            dct:modified ${sparqlEscapeDateTime(now)} ;
            dct:issued ${sparqlEscapeDateTime(now)} ;
            dcat:downloadURL <${HOST_DOMAIN}/files/${logicalFileUuid}/download> ;
            dcat:byteSize ${sparqlEscapeInt(size)} ;
            dct:format ${sparqlEscapeString(format)} ;
            dct:title ?title .
          <${this.uri}> dcat:distribution <${distributionUri}> .
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
   * Deprecate the previous dataset
   * 1/ search for any previous dataset that handles about the same meeting
   * 2/ mark it as prov:wasRevisionOf of the current dataset
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
          ?meeting a <${VERGADERACTIVITEIT_TYPE}> .
        }
        GRAPH <${PUBLIC_GRAPH}> {
          ?dataset a dcat:Dataset ;
            dct:subject ?meeting .
          FILTER NOT EXISTS { ?newerVersion prov:wasRevisionOf ?dataset . }
        }
      } LIMIT 1
    `);

    if (result.results.bindings.length) {
      const binding = result.results.bindings[0];
      const previousDataset = binding['dataset'].value;
      console.log(`Found previous dataset <${previousDataset}>`);

      await update(`
        PREFIX prov: <http://www.w3.org/ns/prov#>

        INSERT DATA {
          GRAPH <${this.graph}> {
            <${this.uri}> prov:wasRevisionOf <${previousDataset}> .
          }
        }
      `);

      await removeDeprecatedTriples(previousDataset);
      await deprecateDistributions(previousDataset);
    }
  }

  /**
   * Add the dataset to the catalog
   *
   * @private
  */
  async addToCatalog() {
    await update(`
    PREFIX dct: <http://purl.org/dc/terms/>

    DELETE WHERE {
      GRAPH <${PUBLIC_GRAPH}> {
        <${BESLUITVORMING_CATALOG_URI}> dct:modified ?modified .
      }
    }
  `);

    await update(`
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dcat: <http://www.w3.org/ns/dcat#>

      INSERT DATA {
        GRAPH <${PUBLIC_GRAPH}> {
          <${BESLUITVORMING_CATALOG_URI}> dct:modified ${sparqlEscapeDateTime(Date.now())} ;
          dcat:dataset <${this.uri}> .
        }
      }
    `);

    console.log(`Dataset <${this.uri}> added to catalog <${BESLUITVORMING_CATALOG_URI}>.`);
  }
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
      GRAPH <${PUBLIC_GRAPH}> {
        ?physicalFileUri a nfo:FileDataObject ;
          nie:dataSource ?logicalFileUri .
        <${previousDataset}> dcat:distribution ?distribution .
        ?distribution dct:type <${DATASET_NEWSITEM_TYPE}> ;
            dct:subject ?physicalFileUri .
      }
    }
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];
    const physicalFileUri = b['physicalFileUri'].value;
    const ttlFile = physicalFileUri.replace('share://', '/share/');
    await deleteTriplesFromTtl(ttlFile, PUBLIC_GRAPH);

  }
}

async function deprecateDistributions(previousDataset) {
  console.log(`Deprecating distributions belonging to previous dataset <${previousDataset}>`);
  await update(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>

    DELETE {
      GRAPH <${PUBLIC_GRAPH}> {
        <${previousDataset}> dct:modified ?datasetModifiedDate .
        ?distribution dcat:downloadURL ?downloadURL ;
          dct:modified ?distributionModifiedDate .
      }
    } WHERE {
      GRAPH <${PUBLIC_GRAPH}> {
        <${previousDataset}> dcat:distribution ?distribution ;
          dct:modified ?datasetModifiedDate .
        ?distribution dcat:downloadURL ?downloadURL ;
          dct:modified ?distributionModifiedDate .
      }
    }
  `);

  const now = Date.now();

  await update(`
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dct:  <http://purl.org/dc/terms/>

    INSERT {
      GRAPH <${PUBLIC_GRAPH}> {
        ?distribution dct:modified ${sparqlEscapeDateTime(now)} .
        <${previousDataset}> dct:modified ${sparqlEscapeDateTime(now)} .
      }
    } WHERE {
      GRAPH <${PUBLIC_GRAPH}> {
        <${previousDataset}> dcat:distribution ?distribution .
      }
    }
  `);
}


export default Dataset;
