import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';

import {
  UPDATE_BATCH_SIZE,
  SELECT_BATCH_SIZE,
  PUBLIC_GRAPH
} from '../config';

/**
 * Move all triples of one graph to another graph
 *
 * @public
 */
async function moveGraph(source, target) {
  const triples = await getGraphTriples(source);
  await insertInGraph(triples, target);
  await removeGraph(source);
}

/**
* Get all the triples from the given graph
*
* @public
*/
async function getGraphTriples(graph) {
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
      LIMIT ${SELECT_BATCH_SIZE} OFFSET %OFFSET
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
    const statements = toStatements(batch);
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
    return `${subject} ${predicate} ${object} . \n`;
  }).join('');
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
  getGraphTriples,
  toStatements
};
