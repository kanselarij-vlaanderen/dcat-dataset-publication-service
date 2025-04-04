import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import Dataset from './dataset';
import { createEmailOnFailure } from './email';
import { MU_APPLICATION_GRAPH, RELEASE_TASK_STATUSES, HOST_DOMAIN } from '../config';

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
      await this.persistStatus(RELEASE_TASK_STATUSES.RELEASING);
      await this.release();
      await this.persistStatus(RELEASE_TASK_STATUSES.SUCCESS);
      const nextTask = await getNextReleaseTask();
      if (nextTask) {
        nextTask.execute();
      }
    } catch (e) {
      await this.closeWithFailure();
      console.log(`Something went wrong while processing the release task.`);
      console.log(e);
      await createEmailOnFailure(
        "A release task has fully failed in dcat-dataset-publication",
        `environment: ${HOST_DOMAIN}\t\nDetail of error: ${e?.message || "no details available"}\t\n
        This error will fully block this and future releases and needs to be fixed manually!`
      );
    }
  }

  /**
   * Function to release a dataset
   * The release of a dataset is done in 3 steps:
   * A. prepare the DCAT resources
   * B. deprecate the previous dataset
   * C. release the new dataset
   *
   * @private
  */
  async release() {
    console.log(`Creating new dataset containing triples from graph <${this.source}>`);
    const dataset = new Dataset(this.source);

    console.log(`Preparing dataset distributions...`);
    await dataset.prepare();

    console.log(`Linking dataset <${dataset.uri}> to task <${this.uri}> ...`);
    await this.linkDataset(dataset.uri);

    console.log(`Deprecating the previous dataset of dataset <${dataset.uri}> if there is any...`);
    await dataset.deprecatePrevious();

    console.log(`Releasing new dataset <${dataset.uri}> ...`);
    await dataset.release();
  };

  /**
  * Close the sync task with a failure status
  *
  * @private
 */
  async closeWithFailure() {
    console.log(`Something went wrong while processing the release task.`);
    await this.persistStatus(RELEASE_TASK_STATUSES.FAILED);
    logFailureResolutionManual(this.uri);
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
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s WHERE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        ?s a ext:ReleaseTask ;
         dct:created ?created ;
         adms:status <${RELEASE_TASK_STATUSES.RELEASING}> .
      }
    } ORDER BY ?created LIMIT 1
  `);

  return result.results.bindings.length ? { uri: result.results.bindings[0]['s'] } : null;
}

/**
 * Get the next release task with the earliest creation date that has not started yet.
 * A release task is only returned if there are no failed tasks. A failure blocks the
 * entire release task queue to ensure tasks are executed in order.
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
          adms:status <${RELEASE_TASK_STATUSES.READY_FOR_RELEASE}> ;
          dct:source ?source ;
          dct:created ?created .
        FILTER NOT EXISTS { ?t a ext:ReleaseTask ; adms:status <${RELEASE_TASK_STATUSES.FAILED}> . }
      }
    } ORDER BY ?created LIMIT 1
  `);

  if (result.results.bindings.length) {
    const b = result.results.bindings[0];

    return new ReleaseTask({
      uri: b['s'].value,
      source: b['source'].value,
      status: RELEASE_TASK_STATUSES.READY_FOR_RELEASE,
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

/**
 * Get the failed release task ordered by creation date.
 * Normally there will only be 1 failed task since
 * a failure blocks the entire queue of release tasks.
 *
 * @public
*/
async function getFailedTask() {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?s ?source ?created WHERE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        ?s a ext:ReleaseTask ;
          adms:status <${RELEASE_TASK_STATUSES.FAILED}> ;
          dct:source ?source ;
          dct:created ?created .
      }
    } ORDER BY ?created LIMIT 1
  `);


  if (result.results.bindings.length) {
    const b = result.results.bindings[0];

    return new ReleaseTask({
      uri: b['s'].value,
      source: b['source'].value,
      status: RELEASE_TASK_STATUSES.FAILED,
      created: new Date(Date.parse(b['created'].value))
    });
  } else {
    return null;
  }
}

/**
 * Prints a manual to help user resolve a failed task state
 *
 * @public
*/
function logFailureResolutionManual(failedTask) {
  console.log(`Task <${failedTask}> failed. This failure will block the entire release queue to ensure release tasks are executed in order. Manually resolve the status of the failed task and restart this service to resume execution.`);
  console.log('Execute the following SPARQL query to reset the task status to ready-for-release');
  console.log(`
    PREFIX adms: <http://www.w3.org/ns/adms#>
    DELETE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        <${failedTask}> adms:status ?status .
      }
    } INSERT {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        <${failedTask}> adms:status <${RELEASE_TASK_STATUSES.READY_FOR_RELEASE}> .
      }
    } WHERE {
      GRAPH <${MU_APPLICATION_GRAPH}> {
        <${failedTask}> adms:status ?status .
      }
    }
  `);
}

export default ReleaseTask;
export {
  getNextReleaseTask,
  getRunningReleaseTask,
  getFailedTask,
  logFailureResolutionManual
};
