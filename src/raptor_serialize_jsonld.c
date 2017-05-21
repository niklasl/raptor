/* -*- Mode: c; c-basic-offset: 2 -*-
 *
 * raptor_serialize_jsonld.c - JSON-LD serializer
 *
 * Initial adaptation from JSON serializer by Niklas Lindström in 2017.
 *
 * Copyright (C) 2008-2009, David Beckett http://www.dajobe.org/
 *
 * This package is Free Software and part of Redland http://librdf.org/
 *
 * It is licensed under the following three licenses as alternatives:
 *   1. GNU Lesser General Public License (LGPL) V2.1 or any newer version
 *   2. GNU General Public License (GPL) V2 or any newer version
 *   3. Apache License, V2.0 or any newer version
 *
 * You may not use this file except in compliance with at least one of
 * the above three licenses.
 *
 * See LICENSE.html or LICENSE.txt at the top of this package for the
 * complete terms and further detail along with the license texts for
 * the licenses in COPYING.LIB, COPYING and LICENSE-2.0.txt respectively.
 *
 */

#ifdef HAVE_CONFIG_H
#include <raptor_config.h>
#endif

#include <stdio.h>
#include <string.h>
#include <ctype.h>
/* for ptrdiff_t */
#ifdef HAVE_STDDEF_H
#include <stddef.h>
#endif
#include <stdarg.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif

/* Raptor includes */
#include "raptor2.h"
#include "raptor_internal.h"


/*
 * Raptor JSON serializer object
 */
typedef struct {

  int need_subject_comma;

  /* JSON writer object */
  raptor_json_writer* json_writer;

  /* Ordered sequence of triples */
  raptor_avltree* avltree;

  /* Last statement generated (shared pointer) */
  raptor_statement* last_statement;

  int need_object_comma;

} raptor_jsonld_szr_context;


struct raptor_json_writer_s {
  raptor_world* world;

  raptor_uri* base_uri;

  /* outputting to this iostream */
  raptor_iostream *iostr;

  /* current indent */
  int indent;

  /* indent step */
  int indent_step;
};


static int raptor_jsonld_serialize_init(raptor_serializer* serializer,
                                      const char *name);
static void raptor_jsonld_serialize_terminate(raptor_serializer* serializer);
static int raptor_jsonld_serialize_start(raptor_serializer* serializer);
static int raptor_jsonld_serialize_statement(raptor_serializer* serializer,
                                           raptor_statement *statement);
static int raptor_jsonld_serialize_end(raptor_serializer* serializer);
static void raptor_jsonld_serialize_finish_factory(raptor_serializer_factory* factory);

static int
raptor_jsonld_writer_id_key_and_uri_or_bnode(raptor_json_writer* json_writer, raptor_term* term, unsigned int flags);

int raptor_jsonld_writer_literal_object(raptor_json_writer* json_writer,
                                  unsigned char* s, size_t s_len,
                                  unsigned char* lang,
                                  raptor_uri* datatype);

int raptor_jsonld_writer_blank_object(raptor_json_writer* json_writer,
                                const unsigned char* blank,
                                size_t blank_len);

int raptor_jsonld_writer_uri_object(raptor_json_writer* json_writer, raptor_uri* uri);

int raptor_jsonld_writer_term(raptor_json_writer* json_writer, raptor_term *term);

static int
raptor_json_writer_quoted(raptor_json_writer* json_writer,
                          const char *value, size_t value_len);

int raptor_statement_compare_gspo(const raptor_statement *s1,
                                  const raptor_statement *s2);

/*
 * raptor serializer JSON implementation
 */


/* create a new serializer */
static int
raptor_jsonld_serialize_init(raptor_serializer* serializer, const char *name)
{
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;

  /* Default for JSON serializer is absolute URIs */
  /* RAPTOR_OPTIONS_SET_NUMERIC(serializer, RAPTOR_OPTION_RELATIVE_URIS, 0); */

  return 0;
}


/* destroy a serializer */
static void
raptor_jsonld_serialize_terminate(raptor_serializer* serializer)
{
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;

  if(context->json_writer) {
    raptor_free_json_writer(context->json_writer);
    context->json_writer = NULL;
  }

  if(context->avltree) {
    raptor_free_avltree(context->avltree);
    context->avltree = NULL;
  }
}


static int
raptor_jsonld_serialize_start(raptor_serializer* serializer)
{
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;
  raptor_uri* base_uri;
  char* value;

  base_uri = RAPTOR_OPTIONS_GET_NUMERIC(serializer, RAPTOR_OPTION_RELATIVE_URIS)
             ? serializer->base_uri : NULL;

  context->json_writer = raptor_new_json_writer(serializer->world,
                                                base_uri,
                                                serializer->iostream);
  if(!context->json_writer)
    return 1;

  context->avltree = raptor_new_avltree(
      (raptor_data_compare_handler)raptor_statement_compare_gspo,
      (raptor_data_free_handler)raptor_free_statement,
      0);
  if(!context->avltree) {
    raptor_free_json_writer(context->json_writer);
    context->json_writer = NULL;
    return 1;
  }

  /* start callback */
  value = RAPTOR_OPTIONS_GET_STRING(serializer, RAPTOR_OPTION_JSON_CALLBACK);
  if(value) {
    raptor_iostream_string_write(value, serializer->iostream);
    raptor_iostream_write_byte('(', serializer->iostream);
  }

  return 0;
}


static int
raptor_jsonld_serialize_statement(raptor_serializer* serializer,
                                raptor_statement *statement)
{
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;

  raptor_statement* s = raptor_statement_copy(statement);
  if(!s)
    return 1;
  return raptor_avltree_add(context->avltree, s);
}


/* return 0 to abort visit */
static int
raptor_jsonld_serialize_avltree_visit(int depth, void* data, void *user_data)
{
  raptor_serializer* serializer = (raptor_serializer*)user_data;
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;

  raptor_statement* statement = (raptor_statement*)data;
  raptor_statement* s1 = statement;
  raptor_statement* s2 = context->last_statement;
  int new_graph = 0;
  int new_subject = 0;
  int new_predicate = 0;
  unsigned int flags = RAPTOR_ESCAPED_WRITE_JSON_LITERAL;

  if(s2) {
    new_graph = s1->graph != s2->graph && !raptor_term_equals(s1->graph, s2->graph);

    new_subject = !raptor_term_equals(s1->subject, s2->subject);

    /* add "@graph" object and start array */
    /* TODO: unless default graph? */
    if(new_graph) {
      raptor_json_writer_end_block(context->json_writer, ']');
      raptor_json_writer_newline(context->json_writer);

      raptor_json_writer_end_block(context->json_writer, '}');
      raptor_json_writer_newline(context->json_writer);

      context->need_subject_comma = 1;
    }

    if(new_subject) {
      /* end last predicate */
      raptor_json_writer_newline(context->json_writer);

      raptor_json_writer_end_block(context->json_writer, ']');
      raptor_json_writer_newline(context->json_writer);

      /* end last statement */
      raptor_json_writer_end_block(context->json_writer, '}');
      raptor_json_writer_newline(context->json_writer);

      context->need_subject_comma = 1;
      context->need_object_comma = 0;
    }
  } else {
    new_graph = 1;
    new_subject = 1;
  }

  if(new_graph) {
    if(context->need_subject_comma) {
      raptor_iostream_write_byte(',', serializer->iostream);
    }
    context->need_subject_comma = 0;
    raptor_json_writer_start_block(context->json_writer, '{');
    /* new_graph is true also for default (NULL) graph  */
    if (s1->graph != NULL) {
      int err = raptor_jsonld_writer_id_key_and_uri_or_bnode(context->json_writer,
                                                            s1->graph, flags);
      if (err == 1) {
        raptor_log_error_formatted(serializer->world, RAPTOR_LOG_LEVEL_ERROR,
                                    NULL,
                                    "Triple has unsupported graph term type %u",
                                    s1->graph->type);
      }
      raptor_iostream_counted_string_write(",", 1, serializer->iostream);
      raptor_json_writer_newline(context->json_writer);
    }
    raptor_iostream_counted_string_write("\"@graph\":", 9,
                                        serializer->iostream);
    raptor_json_writer_start_block(context->json_writer, '[');
    raptor_json_writer_newline(context->json_writer);
  }

  if(new_subject) {
    if(context->need_subject_comma) {
      raptor_iostream_write_byte(',', serializer->iostream);
    }
    raptor_json_writer_start_block(context->json_writer, '{');
    raptor_json_writer_newline(context->json_writer);

    /* start triple */

    /* subject */
    int err = raptor_jsonld_writer_id_key_and_uri_or_bnode(context->json_writer,
                                                           s1->subject, flags);
    if (err == 1) {
      raptor_log_error_formatted(serializer->world, RAPTOR_LOG_LEVEL_ERROR,
                                  NULL,
                                  "Triple has unsupported subject term type %u",
                                  s1->subject->type);
    }
    raptor_iostream_counted_string_write(",", 1, serializer->iostream);
    raptor_json_writer_newline(context->json_writer);
  }


  /* predicate */
  if(context->last_statement) {
    if(new_subject)
      new_predicate = 1;
    else {
      new_predicate = !raptor_uri_equals(s1->predicate->value.uri,
                                         s2->predicate->value.uri);
      if(new_predicate) {
        raptor_json_writer_newline(context->json_writer);
        raptor_json_writer_end_block(context->json_writer, ']');
        raptor_iostream_write_byte(',', serializer->iostream);
        raptor_json_writer_newline(context->json_writer);
      }
    }
  } else
    new_predicate = 1;

  if(new_predicate) {
    /* start predicate */

    raptor_json_writer_key_uri_value(context->json_writer,
                                   NULL, 0,
                                   s1->predicate->value.uri);
    raptor_iostream_counted_string_write(" : ", 3, serializer->iostream);
    raptor_json_writer_start_block(context->json_writer, '[');
    raptor_iostream_write_byte(' ', serializer->iostream);

    context->need_object_comma = 0;
  }

  if(context->need_object_comma) {
    raptor_iostream_write_byte(',', serializer->iostream);
    raptor_json_writer_newline(context->json_writer);
  }

  /* object */
  raptor_jsonld_writer_term(context->json_writer, s1->object);
  if(s1->object->type != RAPTOR_TERM_TYPE_LITERAL)
    raptor_json_writer_newline(context->json_writer);

  /* end triple */

  context->need_object_comma = 1;
  context->last_statement = statement;

  return 1;
}

static int
raptor_jsonld_writer_id_key_and_uri_or_bnode(raptor_json_writer* json_writer, raptor_term* term, unsigned int flags)
{
  raptor_iostream_counted_string_write("\"@id\": ", 7, json_writer->iostr);

  switch(term->type) {
    case RAPTOR_TERM_TYPE_URI:
      raptor_json_writer_key_uri_value(json_writer,
                                        NULL, 0,
                                        term->value.uri);
      break;

    case RAPTOR_TERM_TYPE_BLANK:
      raptor_iostream_counted_string_write("\"_:", 3, json_writer->iostr);
      raptor_string_escaped_write(term->value.blank.string, 0,
                                  '"', flags,
                                  json_writer->iostr);
      raptor_iostream_write_byte('"', json_writer->iostr);
      break;

    case RAPTOR_TERM_TYPE_LITERAL:
    case RAPTOR_TERM_TYPE_UNKNOWN:
    default:
      return 1;
  }
  return 0;
}


static int
raptor_json_writer_quoted(raptor_json_writer* json_writer,
                          const char *value, size_t value_len)
{
  int rc = 0;

  if(!value) {
    raptor_iostream_counted_string_write("\"\"", 2, json_writer->iostr);
    return 0;
  }

  raptor_iostream_write_byte('\"', json_writer->iostr);
  rc = raptor_string_escaped_write((const unsigned char*)value, value_len,
                                   '"', RAPTOR_ESCAPED_WRITE_JSON_LITERAL,
                                   json_writer->iostr);
  raptor_iostream_write_byte('\"', json_writer->iostr);

  return rc;
}


int
raptor_jsonld_writer_literal_object(raptor_json_writer* json_writer,
                                  unsigned char* s, size_t s_len,
                                  unsigned char* lang,
                                  raptor_uri* datatype)
{
  raptor_json_writer_start_block(json_writer, '{');
  raptor_json_writer_newline(json_writer);

  raptor_json_writer_key_value(json_writer, "@value", 6, (const char*)s, s_len);

  if(datatype || lang) {
    raptor_iostream_write_byte(',', json_writer->iostr);
    raptor_json_writer_newline(json_writer);

    if(datatype)
      raptor_json_writer_key_uri_value(json_writer, "@type", 5, datatype);

    if(lang) {
      if(datatype) {
        raptor_iostream_write_byte(',', json_writer->iostr);
        raptor_json_writer_newline(json_writer);
      }

      raptor_json_writer_key_value(json_writer, "@language", 9,
                                   (const char*)lang, 0);
    }
  }

  raptor_json_writer_newline(json_writer);
  raptor_json_writer_end_block(json_writer, '}');
  raptor_json_writer_newline(json_writer);

  return 0;
}


int
raptor_jsonld_writer_blank_object(raptor_json_writer* json_writer,
                                const unsigned char* blank,
                                size_t blank_len)
{
  raptor_json_writer_start_block(json_writer, '{');
  raptor_json_writer_newline(json_writer);

  raptor_iostream_counted_string_write("\"@id\" : \"_:", 11,
                                       json_writer->iostr);
  raptor_iostream_counted_string_write((const char*)blank, blank_len,
                                       json_writer->iostr);
  raptor_iostream_counted_string_write("\",", 2, json_writer->iostr);
  raptor_json_writer_newline(json_writer);

  raptor_json_writer_end_block(json_writer, '}');
  return 0;
}


int
raptor_jsonld_writer_uri_object(raptor_json_writer* json_writer,
                              raptor_uri* uri)
{
  raptor_json_writer_start_block(json_writer, '{');
  raptor_json_writer_newline(json_writer);

  raptor_json_writer_key_uri_value(json_writer, "@id", 3, uri);
  raptor_json_writer_newline(json_writer);

  raptor_json_writer_end_block(json_writer, '}');

  return 0;
}


int
raptor_jsonld_writer_term(raptor_json_writer* json_writer,
                        raptor_term *term)
{
  int rc = 0;

  switch(term->type) {
    case RAPTOR_TERM_TYPE_URI:
      rc = raptor_jsonld_writer_uri_object(json_writer, term->value.uri);
      break;

    case RAPTOR_TERM_TYPE_LITERAL:
      rc = raptor_jsonld_writer_literal_object(json_writer,
                                             term->value.literal.string,
                                             term->value.literal.string_len,
                                             term->value.literal.language,
                                             term->value.literal.datatype);
      break;

    case RAPTOR_TERM_TYPE_BLANK:
      rc = raptor_jsonld_writer_blank_object(json_writer,
                                           term->value.blank.string,
                                           term->value.blank.string_len);
      break;

    case RAPTOR_TERM_TYPE_UNKNOWN:
      default:
        raptor_log_error_formatted(json_writer->world, RAPTOR_LOG_LEVEL_ERROR,
                                   NULL,
                                   "Triple has unsupported term type %u",
                                   term->type);
        rc = 1;
        break;
  }

  return rc;
}


static int
raptor_jsonld_serialize_end(raptor_serializer* serializer)
{
  raptor_jsonld_szr_context* context = (raptor_jsonld_szr_context*)serializer->context;
  char* value;

  raptor_json_writer_newline(context->json_writer);

  /* start outer object */
  raptor_json_writer_start_block(context->json_writer, '[');
  raptor_json_writer_newline(context->json_writer);

  raptor_avltree_visit(context->avltree,
                        raptor_jsonld_serialize_avltree_visit,
                        serializer);

  /* end last triples block */
  if(context->last_statement) {
    raptor_json_writer_newline(context->json_writer);
    raptor_json_writer_end_block(context->json_writer, ']');
    raptor_json_writer_newline(context->json_writer);

    raptor_json_writer_end_block(context->json_writer, '}');
    raptor_json_writer_newline(context->json_writer);
  }

  /* end last graph block */
  /* TODO: unless default graph? */
  if(1) {
  raptor_json_writer_newline(context->json_writer);
  raptor_json_writer_end_block(context->json_writer, ']');
  raptor_json_writer_newline(context->json_writer);
  raptor_json_writer_end_block(context->json_writer, '}');
  raptor_json_writer_newline(context->json_writer);
  }


  value = RAPTOR_OPTIONS_GET_STRING(serializer, RAPTOR_OPTION_JSON_EXTRA_DATA);
  if(value) {
    raptor_iostream_write_byte(',', serializer->iostream);
    raptor_json_writer_newline(context->json_writer);
    raptor_iostream_string_write(value, serializer->iostream);
    raptor_json_writer_newline(context->json_writer);
  }


  /* end outer object */
  raptor_json_writer_end_block(context->json_writer, ']');
  raptor_json_writer_newline(context->json_writer);

  /* end callback */
  if(RAPTOR_OPTIONS_GET_STRING(serializer, RAPTOR_OPTION_JSON_CALLBACK))
    raptor_iostream_counted_string_write((const unsigned char*)");", 2,
                                         serializer->iostream);

  return 0;
}


static void
raptor_jsonld_serialize_finish_factory(raptor_serializer_factory* factory)
{
  /* NOP */
}


/**
 * raptor_statement_compare_gspo:
 * @s1: first statement
 * @s2: second statement
 *
 * Compare a pair of #raptor_statement
 *
 * Uses raptor_term_compare() to check ordering between subjects,
 * predicates and objects of statements.
 * 
 * Return value: <0 if s1 is before s2, 0 if equal, >0 if s1 is after s2
 */
int
raptor_statement_compare_gspo(const raptor_statement *s1,
                         const raptor_statement *s2)
{
  int d = 0;

  if(!s1 || !s2) {
    /* If one or both are NULL, return a stable comparison order */
    ptrdiff_t pd = (s2 - s1);

    return (pd > 0) - (pd < 0);
  }

  d = raptor_term_compare(s1->graph, s2->graph);
  if(d)
    return d;

  d = raptor_term_compare(s1->subject, s2->subject);
  if(d)
    return d;

  d = raptor_term_compare(s1->predicate, s2->predicate);
  if(d)
    return d;

  d = raptor_term_compare(s1->object, s2->object);

  return d;
}



static const char* const jsonld_names[2] = { "jsonld", NULL};

static const char* const jsonld_uri_strings[2] = {
  "http://json-ld.org/",
  NULL
};

#define JSONLD_RESOURCE_TYPES_COUNT 1
static const raptor_type_q jsonld_types[JSONLD_RESOURCE_TYPES_COUNT + 1] = {
  { "application/ld+json", 16, 10},
  { NULL, 0, 0}
};

static int
raptor_jsonld_serializer_register_factory(raptor_serializer_factory *factory)
{
  factory->desc.names = jsonld_names;
  factory->desc.mime_types = jsonld_types;

  factory->desc.label = "JSON-LD Expanded Form";
  factory->desc.uri_strings = jsonld_uri_strings;

  factory->context_length     = sizeof(raptor_jsonld_szr_context);

  factory->init                = raptor_jsonld_serialize_init;
  factory->terminate           = raptor_jsonld_serialize_terminate;
  factory->declare_namespace   = NULL;
  factory->declare_namespace_from_namespace   = NULL;
  factory->serialize_start     = raptor_jsonld_serialize_start;
  factory->serialize_statement = raptor_jsonld_serialize_statement;
  factory->serialize_end       = raptor_jsonld_serialize_end;
  factory->finish_factory      = raptor_jsonld_serialize_finish_factory;

  return 0;
}


int
raptor_init_serializer_jsonld(raptor_world* world)
{
  int rc;

  rc = !raptor_serializer_register_factory(world,
                                           &raptor_jsonld_serializer_register_factory);

  return rc;
}
