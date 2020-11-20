import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import Dataset from './dataset';
import { sparqlEscapeDateTime } from 'mu';

import { BESLUITVORMING_CATALOG_URI, MU_APPLICATION_GRAPH, PUBLIC_GRAPH } from '../config';

const TASK_NOT_STARTED_STATUS = 'http://kanselarij.vo.data.gift/release-task-statuses/not-started';
const TASK_READY_STATUS = 'http://kanselarij.vo.data.gift/release-task-statuses/ready-for-release';
const TASK_ONGOING_STATUS = 'http://kanselarij.vo.data.gift/release-task-statuses/releasing';
const TASK_SUCCESS_STATUS = 'http://kanselarij.vo.data.gift/release-task-statuses/success';
const TASK_FAILED_STATUS = 'http://kanselarij.vo.data.gift/release-task-statuses/failed';

class ReleaseTask {
  constructor({ uri, source, created, status }) {
    /** Uri of the release task */
    this.uri = uri;

    /**
     * Uri of the temporary graph where the data should be retrieved from
    */
    this.source = source;

    /**
     * Datetime as Data object when the task was created in the triplestore
    */
    this.created = created;

    /**
     * Current status of the release task as stored in the triplestore
    */
    this.status = status;

  }

  /**
   * Persists the given status as task status in the triple store
   *
   * @param status {string} URI of the task status
   * @private
  */
  async persistStatus(status) {
    this.status = status;

    await update(`
      PREFIX adms: <http://www.w3.org/ns/adms#>

      DELETE WHERE {
        GRAPH <${MU_APPLICATION_GRAPH}> {
          <${this.uri}> adms:status ?status .
        }
      }
    `);

    await update(`
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      INSERT {
        GRAPH <${MU_APPLICATION_GRAPH}> {
          <${this.uri}> adms:status <${this.status}> .
        }
      } WHERE {
        GRAPH <${MU_APPLICATION_GRAPH}> {
          <${this.uri}> a ext:ReleaseTask .
        }
      }
    `);
  }

  /**
   * Execute the sync task
   *
   * @public
  */
  async execute() {
    try {
      await this.persistStatus(TASK_ONGOING_STATUS);
      await this.release();
      await this.persistStatus(TASK_SUCCESS_STATUS);
      const nextTask = await getNextReleaseTask();
      if (nextTask) {
        nextTask.execute();
      }
    } catch (e) {
      await this.closeWithFailure();
      console.log(`Something went wrong while processing the release task.`);
      console.log(e);
    }
  }

  /**
   * Function to release a dataset
   * The release of a dataset is done in 3 steps:
   * A. prepare the resources
   * B. deprecate the previous dataset
   * C. release the new dataset
   *
   * @private
  */
  async release() {
    console.log(`Creating new dataset from graph ${this.source}`);
    const dataset = new Dataset(this.source);

    console.log(`Preparing new dataset...`);
    await dataset.prepare();

    console.log(`Linking dataset <${dataset.uri}> to task <${this.uri}> ...`);
    await this.linkDataset(dataset.uri);

    console.log(`Deprecating the previous dataset of dataset <${dataset.uri}> ...`);
    await dataset.deprecatePrevious();

    console.log(`Releasing the new dataset <${dataset.uri}> ...`);
    await dataset.release();

    console.log(`Updating catalog <${BESLUITVORMING_CATALOG_URI}>...`);
    await updateCatalog(dataset.uri);
  };

  /**
  * Close the sync task with a failure status
  *
  * @public
 */
  async closeWithFailure() {
    await this.persistStatus(TASK_FAILED_STATUS);
  }

  /**
   * Link the task to the newly created dataset
   *
   * @private
   */
  async linkDataset(datasetUri) {
    await update(`
    PREFIX prov: <http://www.w3.org/ns/prov#>

    INSERT DATA {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        <${this.uri}> prov:generated <${datasetUri}> .
      }
    }
  `);
  }
}

/**
 * Get the URI of the currently running release task.
 * Null if no task is running.
 *
 * @public
*/
async function getRunningReleaseTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?s WHERE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        ?s a ext:ReleaseTask ;
         adms:status <${TASK_ONGOING_STATUS}> .
      }
    } ORDER BY ?created LIMIT 1
  `);

  return result.results.bindings.length ? { uri: result.results.bindings[0]['s'] } : null;
}

/**
 * Get the next release task with the earliest creation date that has not started yet
 *
 * @public
*/
async function getNextReleaseTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?source ?created WHERE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        ?s a ext:ReleaseTask ;
          adms:status <${TASK_NOT_STARTED_STATUS}> ;
          dct:source ?source ;
          dct:created ?created .
        FILTER NOT EXISTS { ?t a ext:ReleaseTask ; adms:status <${TASK_FAILED_STATUS}> . }
      }
    } ORDER BY ?created LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];

    return new ReleaseTask({
      uri: b['s'].value,
      source: b['source'].value,
      status: TASK_NOT_STARTED_STATUS,
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

async function updateCatalog(datasetUri) {
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
        dcat:dataset <${datasetUri}> .
      }
    }
  `);

  console.log(`Catalog <${BESLUITVORMING_CATALOG_URI}> updated.`);
}


export default ReleaseTask;
export {
  getNextReleaseTask,
  getRunningReleaseTask,
  TASK_NOT_STARTED_STATUS
};
