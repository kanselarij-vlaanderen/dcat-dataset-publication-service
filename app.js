import { app, errorHandler } from 'mu';
import { getRunningReleaseTask, getNextReleaseTask } from './lib/release-task';

app.post('/delta', async function (req, res, next) {
  const isRunning = await getRunningReleaseTask();

  if (!isRunning) {
    const task = await getNextReleaseTask();
    if (task) {
      console.log(`Start releasing new DCAT data`);
      try {
        task.execute();
        return res.status(202).end();
      } catch (e) {
        console.log(`Something went wrong while releasing the data. Closing release task with failure state.`);
        console.trace(e);
        await task.closeWithFailure()();
        return next(new Error(e));
      }
    } else {
      console.log(`No scheduled release task found. Did the insertion of a new task just fail?`);
      return res.status(200).end();
    }
  }
});


app.use(errorHandler);