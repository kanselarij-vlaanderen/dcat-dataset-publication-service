import { app, errorHandler } from 'mu';
import { getRunningReleaseTask, getNextReleaseTask, getFailedTask, logFailureResolutionManual } from './lib/release-task';
import { RELEASE_TASK_STATUSES } from './config'
import flatten from 'lodash.flatten';
import bodyParser from 'body-parser';

// parse application/json
app.use(bodyParser.json());

async function init() {
  // check if any release task is already waiting when starting the service
  const task = await getNextReleaseTask();
  if (task) {
    console.log(`Start releasing new DCAT data on start-up`);
    task.execute(); // errors are handled inside task.execute()
  } else {
    const failedTask = await getFailedTask();
    if (failedTask) {
      logFailureResolutionManual(failedTask.uri);
    } else {
      console.log(`No scheduled release task found on start-up. Waiting for new deltas.`);
    }
  }
}

init();

app.post('/delta', async function (req, res, next) {
  try {
    const isRunning = await getRunningReleaseTask();
  
    if (!isRunning) {
      const delta = req.body;
      const inserts = flatten(delta.map(changeSet => changeSet.inserts));
      const statusTriples = inserts.filter((t) => {
        return t.predicate.value == 'http://www.w3.org/ns/adms#status'
          && t.object.value == RELEASE_TASK_STATUSES.READY_FOR_RELEASE;
      });
  
      if (statusTriples.length) {
        console.log(`Found ${statusTriples.length} release tasks.`);
        const task = await getNextReleaseTask();
        if (task) {
          console.log(`Start releasing new DCAT data`);
          task.execute(); // errors are handled inside task.execute()
          return res.status(202).end();
        } else {
          console.log(`No scheduled release task found.`);
          return res.status(200).end();
        }
      } else {
        console.log(`No insertion of status <${RELEASE_TASK_STATUSES.READY_FOR_RELEASE}> found in the delta message.`);
        return res.status(200).end();
      }
    } else {
      console.log("Another release task is running. Not attempting to parse delta's for new tasks.");
      return res.end("Another release task is running. Not attempting to parse delta's for new tasks.");
    }
  } catch (error) {
    console.log('The processing of the delta message has failed.');
    console.trace(error);
    return res.status(500).end();
  }
});

app.use(errorHandler);
