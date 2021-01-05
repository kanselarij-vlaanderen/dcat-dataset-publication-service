# dcat-dataset-publication-service

Publication service to release DCAT datasets and distributions based on the data in a temporary graph provided by [themis-publication-consumer](http://github.com/kanselarij-vlaanderen/themis-publication-consumer). When the publication service is triggered it processes the data in the temporary graph and creates and publishes a dataset. The documents included in the dataset are published as well. All data is published in the same graph.


## Tutorials
### Add the service to a stack
Add the service to your `docker-compose.yml`:

```
  dcat-dataset-publication:
    image: kanselarij/dcat-dataset-publication-service
    volumes:
      - ./data/files:/share
```

The mounted volume `./data/files` is the location where the documents will be stored.

Next, make the service listen for new conversion tasks. Assuming a delta-notifier is already available in the stack, add the following rules to the delta-notifier's configuration in `./config/delta/rules`.

```javascript
export default [
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://kanselarij.vo.data.gift/release-task-statuses/ready-for-release'
      }
    },
    callback: {
      method: 'POST',
      url: 'http://dcat-dataset-publication/delta'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
];
```

## Reference
### Configuration

The following environment variables can be optionally configured:

* `HOST_DOMAIN (default: https://themis.vlaanderen.be)`: the domain where the Themis application is hosted
* `UPDATE_BATCH_SIZE (default: 10)`: amount of triples to insert/delete in one SPARQL query
* `SELECT_BATCH_SIZE (default: 1000)`: amount of triples to select in one SPARQL query

### Model
#### Used prefixes
| Prefix | URI                                                       |
|--------|-----------------------------------------------------------|
| dct    | http://purl.org/dc/terms/                                 |
| adms   | http://www.w3.org/ns/adms#                                |
| ext    | http://mu.semte.ch/vocabularies/ext                       |


#### Release task
##### Class
`ext:ReleaseTask`
##### Properties
| Name    | Predicate     | Range           | Definition                                                                                                        |
|---------|---------------|-----------------|-------------------------------------------------------------------------------------------------------------------|
| status  | `adms:status` | `rdfs:Resource` | Status of the release task, having value `<http://kanselarij.vo.data.gift/release-task-statuses/ready-for-release>` when this services is triggered |
| created | `dct:created` | `xsd:dateTime`  | Datetime of creation of the task                                                                                  |
| source  | `dct:source`  | `rdfs:Resource` | URI of the graph containing the data to be released                                                               |

#### Release task statuses
The status of the release task will be updated to reflect the progress of the task. The following statuses are known:
* http://kanselarij.vo.data.gift/release-task-statuses/not-started
* http://kanselarij.vo.data.gift/release-task-statuses/preparing-release
* http://kanselarij.vo.data.gift/release-task-statuses/ready-for-release
* http://kanselarij.vo.data.gift/release-task-statuses/releasing
* http://kanselarij.vo.data.gift/release-task-statuses/success
* http://kanselarij.vo.data.gift/release-task-statuses/failed

### Data flow
The service is triggered when the status of a release task is changed to `<http://kanselarij.vo.data.gift/release-task-statuses/ready-for-release>`. Execution of a task consists of the following steps:

1. Generate a ttl file containing the dataset data as provided in the temporary graph linked to the release task
2. Deprecate the previous dataset (if any)
3. Release the new dataset in the `PUBLIC_GRAPH` based on the ttl from step 1
4. Remove the temporary graph

If an error occurs during the release, subsequent dataset releases are blocked.

On successful release of a dataset, the status of the `ext:ReleaseTask` is updated to `success`.

The service makes a core assumption that must be respected at all times maximum 1 release task is running at any moment in time

### API
```
POST /delta
```
Endpoint that receives delta's from the delta-notifier and executes a release task when this task is ready for release. A successfully completed release task will result in the dataset being released.
The endpoint is triggered externally whenever a release task is ready for release and is not supposed to be triggered manually.
