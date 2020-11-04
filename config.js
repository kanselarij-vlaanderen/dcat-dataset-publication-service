const BESLUITVORMING_CATALOG_URI = process.env.BESLUITVORMING_CATALOG_URI || 'http://themis.vlaanderen.be/id/catalog/1e4733c1-7701-4f99-b3db-f5d348a7bc4b';
const VERGADERACTIVITEIT_TYPE = process.env.VERGADERACTIVITEIT_TYPE || 'http://data.vlaanderen.be/ns/besluit#Zitting';
const MU_APPLICATION_GRAPH = 'http://mu.semte.ch/graphs/publication-tasks';

export {
  BESLUITVORMING_CATALOG_URI,
  VERGADERACTIVITEIT_TYPE,
  MU_APPLICATION_GRAPH
};
