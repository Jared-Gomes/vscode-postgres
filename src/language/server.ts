import {
  IPCMessageReader,
  IPCMessageWriter,
  createConnection,
  Connection,
  TextDocuments,
  InitializeResult,
  Diagnostic,
  DiagnosticSeverity,
  TextDocumentPositionParams,
  CompletionItem,
  CompletionItemKind,
  SignatureHelp,
  SignatureInformation,
  ParameterInformation,
  TextDocumentSyncKind,
} from 'vscode-languageserver/node';
import { PgClient } from '../common/connection';
import * as fs from 'fs';
import { Validator } from './validator';
import { IConnection as IDBConnection } from '../common/IConnection';
import { BackwardIterator } from '../common/backwordIterator';
import { SqlQueryManager } from '../queries';
import { TextDocument } from 'vscode-languageserver-textdocument';

export interface ISetConnection {
  connection: IDBConnection;
  documentUri?: string;
}

export interface ExplainResults {
  rowCount: number;
  command: string;
  rows?: any[];
  fields?: any[];
}

export interface DBSchema {
  name: string;
}

export interface DBField {
  attisdropped: boolean;
  attname: string;
  attnum: number;
  attrelid: string;
  data_type: string;
}

export interface DBTable {
  schemaname: string;
  tablename: string;
  is_table: boolean;
  columns: DBField[];
}

export interface DBFunctionsRaw {
  schema: string;
  name: string;
  result_type: string;
  argument_types: string;
  type: string;
  description: string;
}

export interface DBFunctionArgList {
  args: string[];
  description: string;
}

export interface DBFunction {
  schema: string;
  name: string;
  result_type: string;
  overloads: DBFunctionArgList[];
  type: string;
}

export interface Ident {
  isQuoted: boolean;
  name: string;
}

export interface SQLTemplate {
  sql: string;
  parameters: {
    type: 'value' | 'helper' | 'identifier' | 'fragment';
    value: string;
    position: number;
  }[];
  returnType?: string;
  range: {
    start: { line: number; character: number };
    end: { line: number; character: number };
  };
}

export interface ColumnInfo {
  name: string;
  type: string;
}

interface TypeMapping {
  tsType: string;
  pgTypes: string[];
}

const typeCompatibility: TypeMapping[] = [
  {
    tsType: 'number',
    pgTypes: [
      'integer',
      'bigint',
      'numeric',
      'smallint',
      'decimal',
      'real',
      'double precision',
    ],
  },
  {
    tsType: 'string',
    pgTypes: ['text', 'varchar', 'char', 'character varying', 'name', 'uuid'],
  },
  {
    tsType: 'boolean',
    pgTypes: ['boolean'],
  },
  {
    tsType: 'Date',
    pgTypes: ['timestamp', 'timestamptz', 'date', 'time'],
  },
  {
    tsType: 'any',
    pgTypes: ['json', 'jsonb'],
  },
];

let schemaCache: DBSchema[] = [];
let tableCache: DBTable[] = [];
let functionCache: DBFunction[] = [];
let keywordCache: string[] = [];
let databaseCache: string[] = [];

/**
 * To Debug the language server
 *
 * 1. Start the extension via F5
 * 2. Under vscode Debug pane, switch to "Attach to Language Server"
 * 3. F5
 */

let connection: Connection = createConnection(
  new IPCMessageReader(process),
  new IPCMessageWriter(process),
);
let dbConnection: PgClient = null,
  dbConnOptions: IDBConnection = null;

console.log = connection.console.log.bind(connection.console);
console.error = connection.console.error.bind(connection.console);

let documents: TextDocuments<TextDocument> = new TextDocuments<TextDocument>(
  TextDocument,
);

documents.listen(connection);

let shouldSendDiagnosticRelatedInformation: boolean = false;

connection.onInitialize((_params): InitializeResult => {
  shouldSendDiagnosticRelatedInformation =
    _params.capabilities &&
    _params.capabilities.textDocument &&
    _params.capabilities.textDocument.publishDiagnostics &&
    _params.capabilities.textDocument.publishDiagnostics.relatedInformation;
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Full,
      completionProvider: {
        triggerCharacters: [' ', '.', '"'],
      },
      signatureHelpProvider: {
        triggerCharacters: ['(', ','],
      },
    },
  };
});

/*
.dP"Y8 888888 888888 88   88 88""Yb     8888b.  88""Yb      dP""b8    db     dP""b8 88  88 888888 
`Ybo." 88__     88   88   88 88__dP      8I  Yb 88__dP     dP   `"   dPYb   dP   `" 88  88 88__   
o.`Y8b 88""     88   Y8   8P 88"""       8I  dY 88""Yb     Yb       dP__Yb  Yb      888888 88""   
8bodP' 888888   88   `YbodP' 88         8888Y"  88oodP      YboodP dP""""Yb  YboodP 88  88 888888 
*/
function dbConnectionEnded() {
  dbConnection = null;
  tableCache = [];
  functionCache = [];
  keywordCache = [];
  databaseCache = [];
}

async function setupDBConnection(
  connectionOptions: IDBConnection,
  uri: string,
): Promise<void> {
  if (connectionOptions) {
    // dbConnection = await Database.createConnection(conn);
    if (
      connectionOptions.certPath &&
      fs.existsSync(connectionOptions.certPath)
    ) {
      connectionOptions.ssl = {
        ca: fs.readFileSync(connectionOptions.certPath).toString(),
        rejectUnauthorized: false,
      };
    }

    if (!connectionOptions.database) {
      connectionOptions = {
        label: connectionOptions.label,
        host: connectionOptions.host,
        user: connectionOptions.user,
        password: connectionOptions.password,
        port: connectionOptions.port,
        database: 'postgres',
        multipleStatements: connectionOptions.multipleStatements,
        certPath: connectionOptions.certPath,
        ssl: connectionOptions.ssl,
      };
    }

    if (connectionOptions.ssl === true) {
      connectionOptions.ssl = { rejectUnauthorized: false };
    }

    dbConnection = new PgClient(connectionOptions);
    await dbConnection.connect();
    const versionRes = await dbConnection.query(
      `SELECT current_setting('server_version_num') as ver_num;`,
    );
    let versionNumber = parseInt(versionRes.rows[0].ver_num);
    dbConnection.pg_version = versionNumber;
    dbConnection.on('end', dbConnectionEnded);

    loadCompletionCache(connectionOptions);

    if (uri) {
      let document = documents.get(uri);
      if (
        document &&
        (document.languageId === 'postgres' ||
          document.languageId === 'typescript')
      ) {
        validateTextDocument(document);
      }
    }
  }
  dbConnOptions = connectionOptions;
}

async function loadCompletionCache(connectionOptions: IDBConnection) {
  if (!connectionOptions || !dbConnection) return;
  // setup database caches for schemas, functions, tables, and fields
  let vQueries = SqlQueryManager.getVersionQueries(dbConnection.pg_version);
  try {
    if (connectionOptions.database) {
      let schemas = await dbConnection.query(`
        SELECT nspname as name
        FROM pg_namespace
        WHERE
          nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
          AND nspname not like 'pg_temp_%'
          AND nspname not like 'pg_toast_temp_%'
          AND has_schema_privilege(oid, 'CREATE, USAGE')
        ORDER BY nspname;
        `);
      schemaCache = schemas.rows;
    }
  } catch (err) {
    console.log(err.message);
  }

  try {
    if (connectionOptions.database) {
      /*
      SELECT tablename as name, true as is_table FROM pg_tables WHERE schemaname not in ('information_schema', 'pg_catalog')
      union all
      SELECT viewname as name, false as is_table FROM pg_views WHERE schemaname not in ('information_schema', 'pg_catalog') order by name;
      */
      let tablesAndColumns = await dbConnection.query(`
        SELECT
          tbl.schemaname,
          tbl.tablename,
          tbl.quoted_name,
          tbl.is_table,
          json_agg(a) as columns
        FROM
          (
            SELECT
              n.nspname as schemaname,
              c.relname as tablename,
              (quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as quoted_name,
              true as is_table
            FROM
              pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE
              c.relkind = 'r'
              AND n.nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
              AND n.nspname not like 'pg_temp_%'
              AND n.nspname not like 'pg_toast_temp_%'
              AND c.relnatts > 0
              AND has_schema_privilege(n.oid, 'USAGE') = true
              AND has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') = true
            union all
            SELECT
              n.nspname as schemaname,
              c.relname as tablename,
              (quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as quoted_name,
              false as is_table
            FROM
              pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE
              c.relkind in ('v', 'm')
              AND n.nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
              AND n.nspname not like 'pg_temp_%'
              AND n.nspname not like 'pg_toast_temp_%'
              AND has_schema_privilege(n.oid, 'USAGE') = true
              AND has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') = true
          ) as tbl
          LEFT JOIN (
            SELECT
              attrelid,
              attname,
              format_type(atttypid, atttypmod) as data_type,
              attnum,
              attisdropped
            FROM
              pg_attribute
          ) as a ON (
            a.attrelid = tbl.quoted_name::regclass
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND has_column_privilege(tbl.quoted_name, a.attname, 'SELECT, INSERT, UPDATE, REFERENCES')
          )
        GROUP BY schemaname, tablename, quoted_name, is_table;
        `);
      tableCache = tablesAndColumns.rows;
    }
  } catch (err) {
    console.log(err.message);
  }

  try {
    let functions = await dbConnection.query(vQueries.GetAllFunctions);

    functions.rows.forEach((fn: DBFunctionsRaw) => {
      // return new ColumnNode(this.connection, this.table, column);
      let existing = functionCache.find((f) => f.name === fn.name);
      if (!existing) {
        existing = {
          name: fn.name,
          schema: fn.schema,
          result_type: fn.result_type,
          type: fn.type,
          overloads: [],
        };
        functionCache.push(existing);
      }
      let args = fn.argument_types
        .split(',')
        .filter((a) => a)
        .map<string>((a) => a.trim());
      existing.overloads.push({ args, description: fn.description });
    });
  } catch (err) {
    console.log(err.message);
  }

  try {
    let keywords = await dbConnection.query(`select * from pg_get_keywords();`);
    keywordCache = keywords.rows.map<string>((rw) =>
      rw.word.toLocaleUpperCase(),
    );
  } catch (err) {
    console.log(err.message);
  }

  try {
    let databases = await dbConnection.query(`
    SELECT datname
    FROM pg_database
    WHERE
      datistemplate = false
      AND has_database_privilege(quote_ident(datname), 'TEMP, CONNECT') = true
    ;`);
    databaseCache = databases.rows.map<string>((rw) => rw.datname);
  } catch (err) {
    console.log(err.message);
  }
}

connection.onRequest('set_connection', async function () {
  let newConnection: ISetConnection = arguments[0];
  if (!dbConnOptions && !newConnection.connection) {
    // neither has a connection - just exist
    return;
  }
  if (
    dbConnOptions &&
    newConnection.connection &&
    newConnection.connection.host === dbConnOptions.host &&
    newConnection.connection.database === dbConnOptions.database &&
    newConnection.connection.port === dbConnOptions.port &&
    newConnection.connection.user === dbConnOptions.user
  ) {
    // same connection - just exit
    return;
  }

  if (dbConnection && !dbConnection.is_ended) {
    // kill the connection first
    await dbConnection.end();
  }
  setupDBConnection(newConnection.connection, newConnection.documentUri).catch(
    (err) => {
      console.log(err.message);
    },
  );
});

/*
 dP"Yb  88   88 888888 88""Yb Yb  dP     Yb    dP    db    88     88 8888b.     db    888888 88  dP"Yb  88b 88 
dP   Yb 88   88 88__   88__dP  YbdP       Yb  dP    dPYb   88     88  8I  Yb   dPYb     88   88 dP   Yb 88Yb88 
Yb b dP Y8   8P 88""   88"Yb    8P         YbdP    dP__Yb  88  .o 88  8I  dY  dP__Yb    88   88 Yb   dP 88 Y88 
 `"YoYo `YbodP' 888888 88  Yb  dP           YP    dP""""Yb 88ood8 88 8888Y"  dP""""Yb   88   88  YbodP  88  Y8 
*/
documents.onDidOpen((change) => {
  validateTextDocument(change.document);
});

documents.onDidChangeContent((change) => {
  validateTextDocument(change.document);
});

function extractSQLTemplates(text: string): SQLTemplate[] {
  const templates: SQLTemplate[] = [];

  // Improved regex to match multi-line sql`` templates
  const sqlRegex = /(sql|authSql|tx|authTx)(?:<([^>]+)>)?`([\s\S]*?)`/g;

  let match;
  while ((match = sqlRegex.exec(text)) !== null) {
    const [full, fnName, returnType, query] = match;

    // Calculate correct line and character positions
    const beforeTemplate = text.substring(0, match.index);
    const lines = beforeTemplate.split('\n');
    const startLine = lines.length - 1;
    const startCharacter = lines[lines.length - 1].length;

    // Calculate end position
    const templateContent = text.substring(
      match.index,
      match.index + full.length,
    );
    const templateLines = templateContent.split('\n');
    const endLine = startLine + templateLines.length - 1;
    const endCharacter =
      templateLines.length === 1
        ? startCharacter + full.length
        : templateLines[templateLines.length - 1].length;

    // Parse parameters ${...} within the template, preserving line information
    const parameters: SQLTemplate['parameters'] = [];
    const paramRegex = /\${([^}]*)}/g;

    let paramMatch;
    let lastIndex = 0;
    let currentLine = 0;
    let currentLineOffset = 0;

    while ((paramMatch = paramRegex.exec(query)) !== null) {
      const param = paramMatch[1].trim();

      // Calculate parameter position considering line breaks
      const precedingText = query.substring(lastIndex, paramMatch.index);
      const precedingLines = precedingText.split('\n');
      currentLine += precedingLines.length - 1;

      if (precedingLines.length > 1) {
        currentLineOffset = precedingLines[precedingLines.length - 1].length;
      } else {
        currentLineOffset += precedingText.length;
      }

      // Determine parameter type
      let type: SQLTemplate['parameters'][0]['type'] = 'value';

      if (param.startsWith('sql(')) {
        if (param.includes('[]') || param.includes('{}')) {
          type = 'helper';
        } else {
          type = 'identifier';
        }
      } else if (param.startsWith('sql`')) {
        type = 'fragment';
      }

      parameters.push({
        type,
        value: param,
        position: currentLineOffset,
      });

      lastIndex = paramMatch.index + paramMatch[0].length;
    }

    templates.push({
      sql: query,
      parameters,
      returnType,
      range: {
        start: { line: startLine, character: startCharacter },
        end: { line: endLine, character: endCharacter },
      },
    });
  }

  return templates;
}

function validateParameterType(
  paramValue: string,
  columnType: string,
  variableType?: string,
): string | null {
  // For sql() helpers and fragments, no type checking needed
  if (paramValue.startsWith('sql(') || paramValue.startsWith('sql`')) {
    return null;
  }

  // Try to infer variable type if not provided
  if (!variableType) {
    // Check for common type patterns
    if (paramValue.match(/^[0-9]+$/)) {
      variableType = 'number';
    } else if (paramValue.match(/^['"].*['"]$/)) {
      variableType = 'string';
    } else if (paramValue === 'true' || paramValue === 'false') {
      variableType = 'boolean';
    }
  }

  if (!variableType) {
    return `Unable to infer type for parameter ${paramValue}`;
  }

  // Find compatible PostgreSQL types for this TypeScript type
  const mapping = typeCompatibility.find((t) => t.tsType === variableType);
  if (!mapping) {
    return `Unsupported TypeScript type: ${variableType}`;
  }

  // Check if the column type is compatible
  if (!mapping.pgTypes.includes(columnType)) {
    return `Type mismatch: TypeScript ${variableType} is not compatible with PostgreSQL ${columnType}`;
  }

  return null;
}

function inferParameterTypes(sqlText: string): string[] {
  const parameterTypes: string[] = [];

  // First try to identify the SQL statement type (SELECT, INSERT, UPDATE, etc)
  const statementType = sqlText.trim().split(/\s+/)[0].toUpperCase();

  switch (statementType) {
    case 'SELECT':
      // For WHERE clauses in SELECT statements
      const whereMatch = sqlText.match(
        /WHERE\s+([\s\S]*?)(?:ORDER BY|GROUP BY|LIMIT|$)/i,
      );
      if (whereMatch) {
        const whereClause = whereMatch[1];
        const conditions = whereClause.split(/\s+AND\s+|\s+OR\s+/i);

        for (const condition of conditions) {
          // Match patterns like "column_name = $1" or "column_name > $2"
          const condMatch = condition.match(
            /([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]+\s*\$\d+/,
          );
          if (condMatch) {
            const columnName = condMatch[1];
            // Find column type from tableCache
            for (const table of tableCache) {
              const column = table.columns.find(
                (c) => c.attname === columnName,
              );
              if (column) {
                parameterTypes.push(column.data_type);
                break;
              }
            }
          }
        }
      }
      break;

    case 'INSERT':
      // For INSERT statements, check the column list
      const insertMatch = sqlText.match(
        /INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)/i,
      );
      if (insertMatch) {
        const [_, tableName, columnList, valuesList] = insertMatch;
        const columns = columnList.split(',').map((c) => c.trim());
        const values = valuesList.split(',').map((v) => v.trim());

        const table = tableCache.find((t) => t.tablename === tableName);
        if (table) {
          values.forEach((value) => {
            if (value.startsWith('$')) {
              const columnIndex = values.indexOf(value);
              const column = table.columns.find(
                (c) => c.attname === columns[columnIndex],
              );
              if (column) {
                parameterTypes.push(column.data_type);
              }
            }
          });
        }
      }
      break;

    case 'UPDATE':
      // For UPDATE statements
      const updateMatch = sqlText.match(
        /UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+SET\s+(.*?)(?:WHERE|$)/i,
      );
      if (updateMatch) {
        const [_, tableName, setClause] = updateMatch;
        const table = tableCache.find((t) => t.tablename === tableName);

        if (table) {
          const assignments = setClause.split(',');
          assignments.forEach((assignment) => {
            const [column, value] = assignment.split('=').map((s) => s.trim());
            if (value.startsWith('$')) {
              const tableColumn = table.columns.find(
                (c) => c.attname === column,
              );
              if (tableColumn) {
                parameterTypes.push(tableColumn.data_type);
              }
            }
          });
        }
      }
      break;
  }

  return parameterTypes;
}

var _NL = '\n'.charCodeAt(0);
async function validateTextDocument(textDocument: TextDocument): Promise<void> {
  let diagnostics: Diagnostic[] = [];
  // parse and find issues
  if (dbConnection) {
    if (textDocument.languageId === 'typescript') {
      const text = textDocument.getText();
      const templates = extractSQLTemplates(text);

      for (const template of templates) {
        let sqlText = template.sql;
        const parameterMap = new Map<number, string>();

        // Replace parameters with actual placeholders
        template.parameters.forEach((param, i) => {
          // Handle postgres.js sql() helper function
          if (param.type === 'helper' && param.value.startsWith('sql(')) {
            // For array/object helpers, replace with a valid postgres array/record literal
            const helperMatch = param.value.match(/sql\((.*)\)/);
            if (helperMatch) {
              const helperValue = helperMatch[1].trim();
              if (helperValue.startsWith('[')) {
                sqlText = sqlText.replace(`\${${param.value}}`, `(1)`); // Dummy array
              } else if (helperValue.startsWith('{')) {
                // Extract column names from the helper arguments
                const args = helperValue
                  .split(',')
                  .slice(1) // Skip the first argument (the object)
                  .map((arg) => arg.trim().replace(/['"]/g, '')); // Remove quotes

                if (args.length > 0) {
                  // Create dummy values matching the number of columns
                  const dummyValues = args.map((col) =>
                    typeof col === 'number' ? '1' : "'dummy'",
                  );

                  // Replace with proper INSERT structure
                  sqlText = sqlText.replace(
                    `\${${param.value}}`,
                    `(${args
                      .map((a) => `"${a}"`)
                      .join(', ')}) VALUES (${dummyValues.join(', ')})`,
                  );
                } else {
                  // Fallback if no columns specified
                  sqlText = sqlText.replace(
                    `\${${param.value}}`,
                    `("col") VALUES ('dummy')`,
                  );
                }
              }
            }
          } else {
            // Regular parameter
            const placeholder = `$${i + 1}`;
            parameterMap.set(i + 1, param.value);
            sqlText = sqlText.replace(`\${${param.value}}`, placeholder);
          }
        });

        try {
          // Just use EXPLAIN for syntax validation
          const results = await dbConnection.query(`EXPLAIN ${sqlText}`);

          // Validate parameter types if we can infer them from the query
          if (template.parameters.length > 0) {
            const inferredTypes = inferParameterTypes(sqlText);
            template.parameters.forEach((param, i) => {
              if (param.type === 'value' && inferredTypes[i]) {
                const error = validateParameterType(
                  param.value,
                  inferredTypes[i],
                );
                if (error) {
                  diagnostics.push({
                    severity: DiagnosticSeverity.Warning,
                    range: {
                      start: {
                        line: template.range.start.line,
                        character: param.position,
                      },
                      end: {
                        line: template.range.start.line,
                        character: param.position + param.value.length,
                      },
                    },
                    message: error,
                    source: 'type-check',
                  });
                }
              }
            });
          }
        } catch (err) {
          diagnostics.push({
            severity: DiagnosticSeverity.Error,
            range: template.range,
            message: err.message,
            source: dbConnOptions.host,
          });
        }
      }
    } else if (textDocument.languageId === 'postgres') {
      let sqlText = textDocument.getText();
      if (!sqlText) {
        connection.sendDiagnostics({ uri: textDocument.uri, diagnostics });
        return;
      }
      for (let sql of Validator.prepare_sql(sqlText)) {
        if (!sql.statement) continue;
        let errColumnMod = 0;
        if (sql.statement.trim().toUpperCase().startsWith('EXPLAIN ')) {
          let match = sql.statement.match(/\s*?EXPLAIN\s/gi);
          if (match) {
            for (let i = 0; i < match[0].length; i++) {
              let ch = match[0].charCodeAt(i);
              errColumnMod++;
              if (ch === _NL) {
                errColumnMod = 1;
                sql.line++;
              }
            }
            sql.statement = sql.statement.replace(/\s*?EXPLAIN\s/gi, '');
          }
        }
        try {
          const results = await dbConnection.query(`EXPLAIN ${sql.statement}`);
        } catch (err) {
          // can use err.position (string)
          // corresponds to full position in query "EXPLAIN ${sql.statement}"
          // need to parse out where in parsed statement and lines that it is
          let errPosition = parseInt(err.position) - 9 + errColumnMod; // removes "EXPLAIN " and turn to zero based
          let errLine = 0;
          while (errPosition > sql.lines[errLine].length) {
            errPosition -= sql.lines[errLine].length + 1;
            errLine++;
          }
          // should have the line - and column
          // find next space after position
          let spacePos = errPosition;
          if (errPosition < sql.lines[errLine].length) {
            spacePos = sql.lines[errLine].indexOf(' ', errPosition);
            if (spacePos < 0) {
              spacePos = sql.lines[errLine].length;
            }
          }
          if (errLine === 0) {
            errPosition += sql.column; // add the column back in - only for first line
          }
          diagnostics.push({
            severity: DiagnosticSeverity.Error,
            range: {
              start: { line: sql.line + errLine, character: errPosition },
              end: { line: sql.line + errLine, character: spacePos },
            },
            message: err.message,
            source: dbConnOptions.host,
          });
        }
      }
    }
  }
  connection.sendDiagnostics({ uri: textDocument.uri, diagnostics });
}

/*
 dP""b8  dP"Yb  8888b.  888888      dP""b8  dP"Yb  8b    d8 88""Yb 88     888888 888888 88  dP"Yb  88b 88 
dP   `" dP   Yb  8I  Yb 88__       dP   `" dP   Yb 88b  d88 88__dP 88     88__     88   88 dP   Yb 88Yb88 
Yb      Yb   dP  8I  dY 88""       Yb      Yb   dP 88YbdP88 88"""  88  .o 88""     88   88 Yb   dP 88 Y88 
 YboodP  YbodP  8888Y"  888888      YboodP  YbodP  88 YY 88 88     88ood8 888888   88   88  YbodP  88  Y8 
*/
export interface FieldCompletionItem extends CompletionItem {
  tables?: string[];
}

connection.onCompletion((e: any): CompletionItem[] => {
  let items: FieldCompletionItem[] = [];
  let scenarioFound = false;

  let document = documents.get(e.textDocument.uri);
  if (!document) return items;

  // For TypeScript files, check if we're inside a SQL template
  if (document.languageId === 'typescript') {
    const templates = extractSQLTemplates(document.getText());
    const position = document.offsetAt(e.position);

    const activeTemplate = templates.find(
      (t) =>
        position >= document.offsetAt(t.range.start) &&
        position <= document.offsetAt(t.range.end),
    );

    if (!activeTemplate) return [];

    // Adjust position to be relative to SQL template
    const sqlPosition = {
      line: e.position.line - activeTemplate.range.start.line,
      character: e.position.character - activeTemplate.range.start.character,
    };

    // Use existing completion logic with adjusted position
    return getCompletionItems(sqlPosition, activeTemplate);
  }

  let iterator = new BackwardIterator(
    document,
    e.position.character - 1,
    e.position.line,
  );

  // // look back and grab the text immediately prior to match to table
  // let line = document.getText({
  //   start: {line: e.position.line, character: 0},
  //   end: {line: e.position.line, character: e.position.character}
  // });

  // let prevSpace = line.lastIndexOf(' ', e.position.character - 1) + 1;
  // let keyword = line.substring(prevSpace, e.position.character - 1);

  if (e.context.triggerCharacter === '"') {
    let startingQuotedIdent = iterator.isFowardDQuote();
    if (!startingQuotedIdent) return items;

    iterator.next(); // get passed the starting quote
    if (iterator.isNextPeriod()) {
      // probably a field - get the ident
      let ident = iterator.readIdent();
      let isQuotedIdent = false;
      if (ident.match(/^\".*?\"$/)) {
        isQuotedIdent = true;
        ident = fixQuotedIdent(ident);
      }
      let table = tableCache.find((tbl) => {
        return (
          (isQuotedIdent && tbl.tablename === ident) ||
          (!isQuotedIdent &&
            tbl.tablename.toLocaleLowerCase() == ident.toLocaleLowerCase())
        );
      });

      if (!table) return items;
      table.columns.forEach((field) => {
        items.push({
          label: field.attname,
          kind: CompletionItemKind.Property,
          detail: field.data_type,
        });
      });
    } else {
      // probably a table - list the tables
      tableCache.forEach((table) => {
        items.push({
          label: table.tablename,
          kind: CompletionItemKind.Class,
        });
      });
    }
    return items;
  }

  if (e.context.triggerCharacter === '.') {
    let idents = readIdents(iterator, 3);
    let pos = 0;

    let schema = schemaCache.find((sch) => {
      return (
        (idents[pos].isQuoted && sch.name === idents[pos].name) ||
        (!idents[pos].isQuoted &&
          sch.name.toLocaleLowerCase() == idents[pos].name.toLocaleLowerCase())
      );
    });

    if (!schema) {
      schema = schemaCache.find((sch) => {
        return sch.name == 'public';
      });
    } else {
      pos++;
    }

    if (idents.length == pos) {
      tableCache.forEach((tbl) => {
        if (tbl.schemaname != schema.name) {
          return;
        }
        items.push({
          label: tbl.tablename,
          kind: CompletionItemKind.Class,
          detail: tbl.schemaname !== 'public' ? tbl.schemaname : null,
        });
      });
      return items;
    }

    let table = tableCache.find((tbl) => {
      return (
        (tbl.schemaname == schema.name &&
          idents[pos].isQuoted &&
          tbl.tablename === idents[pos].name) ||
        (!idents[pos].isQuoted &&
          tbl.tablename.toLocaleLowerCase() ==
            idents[pos].name.toLocaleLowerCase())
      );
    });

    if (table) {
      table.columns.forEach((field) => {
        items.push({
          label: field.attname,
          kind: CompletionItemKind.Property,
          detail: field.data_type,
        });
      });
    }
    return items;
  }

  if (!scenarioFound) {
    schemaCache.forEach((schema) => {
      items.push({
        label: schema.name,
        kind: CompletionItemKind.Module,
      });
    });
    tableCache.forEach((table) => {
      items.push({
        label: table.tablename,
        detail: table.schemaname !== 'public' ? table.schemaname : null,
        kind: table.is_table
          ? CompletionItemKind.Class
          : CompletionItemKind.Interface,
        insertText:
          table.schemaname == 'public'
            ? table.tablename
            : table.schemaname + '.' + table.tablename,
      });
      table.columns.forEach((field) => {
        let foundItem = items.find(
          (i) =>
            i.label === field.attname &&
            i.kind === CompletionItemKind.Field &&
            i.detail === field.data_type,
        );
        if (foundItem) {
          foundItem.tables.push(table.tablename);
          foundItem.tables.sort();
          foundItem.documentation = foundItem.tables.join(', ');
        } else {
          items.push({
            label: field.attname,
            kind: CompletionItemKind.Field,
            detail: field.data_type,
            documentation: table.tablename,
            tables: [table.tablename],
          });
        }
      });
    });
    functionCache.forEach((fn) => {
      items.push({
        label: fn.name,
        kind: CompletionItemKind.Function,
        detail: fn.result_type,
        documentation: fn.overloads[0].description,
      });
    });
    keywordCache.forEach((keyword) => {
      items.push({
        label: keyword,
        kind: CompletionItemKind.Keyword,
      });
    });
    databaseCache.forEach((database) => {
      items.push({
        label: database,
        kind: CompletionItemKind.Module,
      });
    });
  }
  return items;
});

function getCompletionItems(
  position: { line: number; character: number },
  template?: SQLTemplate,
): FieldCompletionItem[] {
  let items: FieldCompletionItem[] = [];

  // For SQL templates, adjust position to be relative to the template content
  let sqlPosition = position;
  if (template) {
    // Account for any parameters before this position
    let parameterOffset = 0;
    template.parameters.forEach((param) => {
      if (param.position < position.character) {
        // Add length of ${param} and subtract length of $N placeholder
        parameterOffset += param.value.length + 3 - 2;
      }
    });
    sqlPosition.character -= parameterOffset;
  }

  // Add schema completions
  schemaCache.forEach((schema) => {
    items.push({
      label: schema.name,
      kind: CompletionItemKind.Module,
    });
  });

  // Add table completions
  tableCache.forEach((table) => {
    items.push({
      label: table.tablename,
      detail: table.schemaname !== 'public' ? table.schemaname : null,
      kind: table.is_table
        ? CompletionItemKind.Class
        : CompletionItemKind.Interface,
      insertText:
        table.schemaname == 'public'
          ? table.tablename
          : table.schemaname + '.' + table.tablename,
    });

    // Add column completions
    table.columns.forEach((field) => {
      let foundItem = items.find(
        (i) =>
          i.label === field.attname &&
          i.kind === CompletionItemKind.Field &&
          i.detail === field.data_type,
      );

      if (foundItem) {
        foundItem.tables.push(table.tablename);
        foundItem.tables.sort();
        foundItem.documentation = foundItem.tables.join(', ');
      } else {
        items.push({
          label: field.attname,
          kind: CompletionItemKind.Field,
          detail: field.data_type,
          documentation: table.tablename,
          tables: [table.tablename],
        });
      }
    });
  });

  // Add function completions
  functionCache.forEach((fn) => {
    items.push({
      label: fn.name,
      kind: CompletionItemKind.Function,
      detail: fn.result_type,
      documentation: fn.overloads[0].description,
    });
  });

  // Add keyword completions
  keywordCache.forEach((keyword) => {
    items.push({
      label: keyword,
      kind: CompletionItemKind.Keyword,
    });
  });

  // Add database completions
  databaseCache.forEach((database) => {
    items.push({
      label: database,
      kind: CompletionItemKind.Module,
    });
  });

  // If we're in a template literal, add special completions for parameters
  if (template) {
    items.push({
      label: 'sql',
      kind: CompletionItemKind.Function,
      detail: 'SQL Helper Function',
      documentation: 'Helper function for SQL identifiers and fragments',
    });

    // Add helper completions for arrays and objects
    items.push({
      label: 'sql([...])',
      kind: CompletionItemKind.Snippet,
      detail: 'SQL Array Helper',
      documentation: 'Helper for SQL array parameters',
    });

    items.push({
      label: 'sql({...})',
      kind: CompletionItemKind.Snippet,
      detail: 'SQL Object Helper',
      documentation: 'Helper for SQL object parameters',
    });
  }

  return items;
}

/*
.dP"Y8 88  dP""b8 88b 88    db    888888 88   88 88""Yb 888888 
`Ybo." 88 dP   `" 88Yb88   dPYb     88   88   88 88__dP 88__   
o.`Y8b 88 Yb  "88 88 Y88  dP__Yb    88   Y8   8P 88"Yb  88""   
8bodP' 88  YboodP 88  Y8 dP""""Yb   88   `YbodP' 88  Yb 888888 
*/
connection.onSignatureHelp((positionParams): SignatureHelp => {
  let document = documents.get(positionParams.textDocument.uri);
  let activeSignature = null,
    activeParameter = null,
    signatures: SignatureInformation[] = [];
  if (document) {
    let iterator = new BackwardIterator(
      document,
      positionParams.position.character - 1,
      positionParams.position.line,
    );

    let paramCount = iterator.readArguments();
    if (paramCount < 0) return null;

    let ident = iterator.readIdent();
    if (!ident || ident.match(/^\".*?\"$/)) return null;

    let fn = functionCache.find(
      (f) => f.name.toLocaleLowerCase() === ident.toLocaleLowerCase(),
    );
    if (!fn) return null;

    let overloads = fn.overloads.filter((o) => o.args.length >= paramCount);
    if (!overloads || !overloads.length) return null;

    overloads.forEach((overload) => {
      signatures.push({
        label: `${fn.name}( ${overload.args.join(' , ')} )`,
        documentation: overload.description,
        parameters: overload.args.map<ParameterInformation>((v) => {
          return { label: v };
        }),
      });
    });

    activeSignature = 0;
    activeParameter = Math.min(paramCount, overloads[0].args.length - 1);
  }
  return { signatures, activeSignature, activeParameter };
});

function fixQuotedIdent(str: string): string {
  return str.replace(/^\"/, '').replace(/\"$/, '').replace(/\"\"/, '"');
}

function readIdents(iterator: BackwardIterator, maxlvl: number): Ident[] {
  return iterator.readIdents(maxlvl).map<Ident>((name) => {
    let isQuoted = false;
    if (name.match(/^\".*?\"$/)) {
      isQuoted = true;
      name = fixQuotedIdent(name);
    }
    return { isQuoted: isQuoted, name: name };
  });
}

function inferQueryReturnType(sqlText: string): ColumnInfo[] {
  // Quick parse SELECT statement columns
  const selectMatch = sqlText.match(/SELECT\s+(.*?)\s+FROM/i);
  if (!selectMatch) return null;

  const columns: ColumnInfo[] = [];
  const columnList = selectMatch[1].split(',');

  for (const col of columnList) {
    const colParts = col.trim().split(/\s+AS\s+/i);
    const colName = colParts[colParts.length - 1].trim().replace(/["`]/g, '');

    // Find matching table column type
    let colType = 'any';

    // Check if column is from tableCache
    for (const table of tableCache) {
      const tableCol = table.columns.find((c) => c.attname === colName);
      if (tableCol) {
        colType = tableCol.data_type;
        break;
      }
    }

    columns.push({
      name: colName,
      type: colType,
    });
  }

  return columns;
}

function validateReturnType(
  inferredColumns: ColumnInfo[],
  returnType: string,
): string[] {
  const errors: string[] = [];

  // Parse return type (assuming format like "{ col1: type1, col2: type2 }[]")
  const typeMatch = returnType.match(/{\s*(.*?)\s*}\s*\[\s*\]/);
  if (!typeMatch) {
    errors.push(`Invalid return type format: ${returnType}`);
    return errors;
  }

  const expectedColumns = typeMatch[1].split(',').map((col) => {
    const [name, type] = col.split(':').map((s) => s.trim());
    return { name, type };
  });

  // Check that all expected columns exist in inferred columns
  for (const expected of expectedColumns) {
    const inferred = inferredColumns.find((col) => col.name === expected.name);

    if (!inferred) {
      errors.push(`Missing column: ${expected.name}`);
      continue;
    }

    // Map PostgreSQL types to TypeScript types
    const pgToTs = {
      integer: 'number',
      bigint: 'number',
      numeric: 'number',
      text: 'string',
      varchar: 'string',
      boolean: 'boolean',
      timestamp: 'Date',
      json: 'any',
      jsonb: 'any',
    };

    const expectedTsType = expected.type;
    const inferredTsType = pgToTs[inferred.type] || 'any';

    if (expectedTsType !== inferredTsType && expectedTsType !== 'any') {
      errors.push(
        `Type mismatch for ${expected.name}: expected ${expectedTsType}, got ${inferredTsType}`,
      );
    }
  }

  return errors;
}

// setup the language service
connection.listen();
