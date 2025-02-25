import {
  EMAIL_FROM_ADDRESS,
  EMAIL_TO_ADDRESS_ON_FAILURE,
  EMAIL_GRAPH_URI,
  EMAIL_OUTBOX_URI,
  RESOURCE_BASE_URI,
} from "../config";
import { updateSudo as update } from "@lblod/mu-auth-sudo";
import { uuid, sparqlEscapeUri, sparqlEscapeString } from "mu";

async function createEmailOnFailure(subject, content) {
  if (!EMAIL_TO_ADDRESS_ON_FAILURE) {
    console.log(
      "** Mail not created, there was no email address found to send to on failure **"
    );
    return;
  }

  const id = uuid();
  const uri = `${RESOURCE_BASE_URI}/email/${id}`;

  const queryString = `
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX nmo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#>

INSERT DATA {
  GRAPH ${sparqlEscapeUri(EMAIL_GRAPH_URI)} {
    ${sparqlEscapeUri(uri)} a nmo:Email;
      mu:uuid ${sparqlEscapeString(id)} ;
      nmo:messageFrom ${sparqlEscapeString(EMAIL_FROM_ADDRESS)} ;
      nmo:emailTo ${sparqlEscapeString(EMAIL_TO_ADDRESS_ON_FAILURE)} ;
      nmo:messageSubject ${sparqlEscapeString(subject)} ;
      nmo:plainTextMessageContent ${sparqlEscapeString(content)} ;
      nmo:isPartOf ${sparqlEscapeUri(EMAIL_OUTBOX_URI)} .
 }
}`;
  await update(queryString);
}

export { createEmailOnFailure };
