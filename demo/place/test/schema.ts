/**
 * A minimal JSON Schema validator for the subset `api/openapi.yaml` uses
 * (type, const, enum, properties, required, additionalProperties: false,
 * items, minItems, maxItems, minimum, maximum, minLength, maxLength, pattern,
 * oneOf, $ref). It exists so the contract test needs no extra dependency.
 * An unknown keyword fails loudly, so the spec cannot silently outgrow it.
 */
import { readFileSync } from 'node:fs';
import { parse } from 'yaml';

type Schema = Record<string, unknown>;

const KNOWN = new Set([
  'type',
  'const',
  'enum',
  'properties',
  'required',
  'additionalProperties',
  'items',
  'minItems',
  'maxItems',
  'minimum',
  'maximum',
  'minLength',
  'maxLength',
  'pattern',
  'oneOf',
  '$ref',
  'description',
  'format',
  'discriminator',
]);

export interface OpenApiDoc {
  paths: Record<string, Record<string, unknown>>;
  components: { schemas: Record<string, Schema> };
}

export function loadSpec(path: string): OpenApiDoc {
  return parse(readFileSync(path, 'utf8')) as OpenApiDoc;
}

/** Returns a list of problems; empty means valid. */
export function validate(doc: OpenApiDoc, schema: Schema, value: unknown, at = '$'): string[] {
  for (const k of Object.keys(schema)) if (!KNOWN.has(k)) return [`${at}: validator does not support keyword "${k}"`];
  if (typeof schema.$ref === 'string') {
    const name = schema.$ref.replace('#/components/schemas/', '');
    const target = doc.components.schemas[name];
    if (!target) return [`${at}: unknown $ref ${schema.$ref}`];
    return validate(doc, target, value, at);
  }
  if (Array.isArray(schema.oneOf)) {
    const matches = (schema.oneOf as Schema[]).filter((s) => validate(doc, s, value, at).length === 0);
    return matches.length === 1 ? [] : [`${at}: matches ${matches.length} of oneOf, expected exactly 1`];
  }
  const errs: string[] = [];
  if ('const' in schema && value !== schema.const) errs.push(`${at}: expected ${JSON.stringify(schema.const)}`);
  if (Array.isArray(schema.enum) && !schema.enum.includes(value))
    errs.push(`${at}: ${JSON.stringify(value)} not in enum`);
  if (schema.type !== undefined) {
    const types = Array.isArray(schema.type) ? (schema.type as string[]) : [schema.type as string];
    if (!types.some((t) => isType(value, t)))
      return [...errs, `${at}: expected ${types.join('|')}, got ${JSON.stringify(value)}`];
  }
  if (typeof value === 'string') {
    if (typeof schema.pattern === 'string' && !new RegExp(schema.pattern, 'u').test(value)) {
      errs.push(`${at}: ${JSON.stringify(value)} does not match ${schema.pattern}`);
    }
    if (typeof schema.minLength === 'number' && value.length < schema.minLength)
      errs.push(`${at}: shorter than ${schema.minLength}`);
    if (typeof schema.maxLength === 'number' && value.length > schema.maxLength)
      errs.push(`${at}: longer than ${schema.maxLength}`);
  }
  if (typeof value === 'number') {
    if (typeof schema.minimum === 'number' && value < schema.minimum) errs.push(`${at}: below ${schema.minimum}`);
    if (typeof schema.maximum === 'number' && value > schema.maximum) errs.push(`${at}: above ${schema.maximum}`);
  }
  if (Array.isArray(value)) {
    if (typeof schema.minItems === 'number' && value.length < schema.minItems)
      errs.push(`${at}: fewer than ${schema.minItems} items`);
    if (typeof schema.maxItems === 'number' && value.length > schema.maxItems)
      errs.push(`${at}: more than ${schema.maxItems} items`);
    if (schema.items) {
      for (const [i, v] of value.entries()) errs.push(...validate(doc, schema.items as Schema, v, `${at}[${i}]`));
    }
  }
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    const props = (schema.properties ?? {}) as Record<string, Schema>;
    for (const r of (schema.required ?? []) as string[]) if (!(r in obj)) errs.push(`${at}: missing ${r}`);
    for (const [k, v] of Object.entries(obj)) {
      if (props[k]) errs.push(...validate(doc, props[k], v, `${at}.${k}`));
      else if (schema.additionalProperties === false) errs.push(`${at}: unexpected property ${k}`);
    }
  }
  return errs;
}

function isType(v: unknown, t: string): boolean {
  switch (t) {
    case 'null':
      return v === null;
    case 'integer':
      return Number.isInteger(v);
    case 'number':
      return typeof v === 'number' && Number.isFinite(v);
    case 'array':
      return Array.isArray(v);
    case 'object':
      return v !== null && typeof v === 'object' && !Array.isArray(v);
    default:
      return typeof v === t;
  }
}
