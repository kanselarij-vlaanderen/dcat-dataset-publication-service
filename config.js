const BESLUITVORMING_CATALOG_URI = 'http://themis.vlaanderen.be/id/catalog/1e4733c1-7701-4f99-b3db-f5d348a7bc4b';
const VERGADERACTIVITEIT_TYPE = 'http://data.vlaanderen.be/ns/besluit#Vergaderactiviteit';
const DATASET_TYPE = 'http://themis.vlaanderen.be/id/concept/dataset-type/9119805f-9ee6-4ef1-9ef7-ad8dccc2bf2d';
const DATASET_NEWSITEM_TYPE = 'http://themis.vlaanderen.be/id/concept/distribution-type/dd5bfc23-8f88-4df5-80f6-a9f72e08d7c4';
const DATASET_ATTACHMENT_TYPE = 'http://themis.vlaanderen.be/id/concept/distribution-type/c4d99dde-3df9-4da1-8136-9a3b2de82de4';
const MU_APPLICATION_GRAPH = 'http://mu.semte.ch/graphs/publication-tasks';
const PUBLIC_GRAPH = 'http://mu.semte.ch/graphs/public';
const HOST_DOMAIN = process.env.HOST_DOMAIN || 'https://themis.vlaanderen.be';
const UPDATE_BATCH_SIZE = parseInt(process.env.UPDATE_BATCH_SIZE) || 10;
const SELECT_BATCH_SIZE = parseInt(process.env.SELECT_BATCH_SIZE) || 1000;

export {
  BESLUITVORMING_CATALOG_URI,
  VERGADERACTIVITEIT_TYPE,
  DATASET_TYPE,
  DATASET_NEWSITEM_TYPE,
  DATASET_ATTACHMENT_TYPE,
  MU_APPLICATION_GRAPH,
  PUBLIC_GRAPH,
  HOST_DOMAIN,
  UPDATE_BATCH_SIZE,
  SELECT_BATCH_SIZE
};
