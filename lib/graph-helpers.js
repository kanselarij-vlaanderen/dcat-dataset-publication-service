import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import fs from 'fs-extra';

import {
  UPDATE_BATCH_SIZE,
  SELECT_BATCH_SIZE
} from '../config';

/**
 * Move all triples of one graph to another graph
 *
 * @public
 */
async function moveGraph(source, target) {
  const triples = await getTriples(source);
  console.log(`Copying triples from source graph <${source}> to target graph <${target}>`);
  await insertInGraph(triples, target);

  // following an issue where not all triples were copied, implement an extra check and copy
  console.log(`Verifying if all triples where moved`);
  await compareGraphAndCopyMissingTriples(source, target);

  console.log(`Removing triples from source graph <${source}>`);
  await removeGraph(source);
}

/**
 * Write the given graph to a TTL file on the specified path
 *
 * @public
 */
async function writeToFile(path, graph) {
  const triples = await getTriples(graph);
  const statements = triples.map(t => toTripleStatement(t)).join('\n');
  await fs.writeFile(path, statements);
}

/**
* Get all the triples from the given graph
*
* @private
*/
async function getTriples(graph) {
  let triples = [];
  const count = await countTriples(graph);
  if (count > 0) {
    console.log(`Parsing 0/${count} triples`);
    let offset = 0;
    const query = `
      SELECT * WHERE {
        GRAPH <${graph}> {
          ?subject ?predicate ?object .
        }
      }
      ORDER BY ?subject LIMIT ${SELECT_BATCH_SIZE} OFFSET %OFFSET
    `;

    while (offset < count) {
      const result = await parseBatch(query, offset);
      triples.push(...result);
      offset = offset + SELECT_BATCH_SIZE;
      console.log(`Parsed ${offset < count ? offset : count}/${count} triples`);
    }
  }

  return triples;
}

async function insertInGraph(triples, graph) {
  for (let i = 0; i < triples.length; i += UPDATE_BATCH_SIZE) {
    console.log(`Inserting triples in batch: ${i}-${i + UPDATE_BATCH_SIZE}`);
    const batch = triples.slice(i, i + UPDATE_BATCH_SIZE);
    const statements = batch.map(b => toTripleStatement(b)).join('\n');
    await update(`
      INSERT DATA {
        GRAPH <${graph}> {
            ${statements}
        }
      }
    `);
  }
}

async function removeGraph(graph) {
  const count = await countTriples(graph);
  if (count > 0) {
    console.log(`Deleting 0/${count} triples`);
    let offset = 0;
    const deleteStatement = `
      DELETE {
        GRAPH <${graph}> {
          ?subject ?predicate ?object .
        }
      }
      WHERE {
        GRAPH <${graph}> {
          SELECT ?subject ?predicate ?object
            WHERE { ?subject ?predicate ?object }
            LIMIT ${UPDATE_BATCH_SIZE}
        }
      }
    `;

    while (offset < count) {
      console.log(`Deleting triples in batch: ${offset}-${offset + UPDATE_BATCH_SIZE}`);
      await update(deleteStatement);
      offset = offset + UPDATE_BATCH_SIZE;
    }
  }
}

function toTripleStatement(triple) {
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

  const subject = escape(triple.subject);
  const predicate = escape(triple.predicate);
  const object = escape(triple.object);
  return `${subject} ${predicate} ${object} .`;
}

/**
* Verify all triples in the source graph were moved to the target graph
* If not, do another copy of those triples
*
* @private
*/
async function compareGraphAndCopyMissingTriples(source, target) {
  // this is a copy of the yggdrasil code
  // this also works differently than the current method that gets a list of triples and copies just those
  // if that list is faulty for any reason this should get all the rest of the triples
  const queryResult = await query(`
    SELECT (COUNT(*) as ?count) WHERE {
      GRAPH <${source}> { ?s ?p ?o . }
      FILTER NOT EXISTS {
        GRAPH <${target}> { ?s ?p ?o . }
      }
    }`);
  const count = parseInt(queryResult.results.bindings[0].count.value);
  if (count === 0) {
    console.log('All triples were copied in target graph <${target}>, nothing to do');
    return;
  }
  console.log(`${count} triples in graph <${source}> not found in target graph <${target}>. Going to copy these triples.`);
  const limit = UPDATE_BATCH_SIZE;
  const totalBatches = Math.ceil(count / limit);
  console.log(`Copying ${count} triples in batches of ${UPDATE_BATCH_SIZE}`);
  let currentBatch = 0;
  while (currentBatch < totalBatches) {
    console.log(`Copy triples (batch ${currentBatch + 1}/${totalBatches})`)
      // Note: no OFFSET needed in the subquery. Pagination is inherent since
      // the WHERE clause doesn't match any longer for triples that are copied in the previous batch.
      await update(`
      INSERT {
        GRAPH <${target}> {
          ?resource ?p ?o .
        }
      } WHERE {
        SELECT ?resource ?p ?o WHERE {
          GRAPH <${source}> { ?resource ?p ?o . }
          FILTER NOT EXISTS {
            GRAPH <${target}> { ?resource ?p ?o }
          }
        } LIMIT ${limit}
      }`);
    currentBatch++;
  }
}

/**
* Count the triples in the given graph
*
* @private
*/
async function countTriples(graph) {
  const queryResult = await query(`
        SELECT (COUNT(*) as ?count)
        WHERE {
          GRAPH <${graph}> {
            ?s ?p ?o .
          }
        }
      `);

  return parseInt(queryResult.results.bindings[0].count.value);
}

async function parseBatch(q, offset = 0) {
  const pagedQuery = q.replace('%OFFSET', offset);
  const result = await query(pagedQuery);

  return result.results.bindings.length ? result.results.bindings : null;
}

export {
  moveGraph,
  writeToFile
};
