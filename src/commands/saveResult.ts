import BaseCommand from '../common/baseCommand';
import * as vscode from 'vscode';
import * as EasyXml from 'easyxml';
import * as csv from 'csv-stringify';
import { SaveTableQuickPickItem } from '../common/IConnQuickPick';
import { Global } from '../common/global';
import { QueryResults } from '../common/database';
import { formatFieldValue } from '../common/formatting';

export class saveResultCommand extends BaseCommand {
  async run(uri: vscode.Uri) {
    let results = Global.ResultManager.activeWinResults;
    if (!results) {
      vscode.window.showWarningMessage(
        'Unable to save data - dataset not found',
      );
      return;
    }

    let resultIndex = 0;
    if (results.length > 1) {
      let tables: SaveTableQuickPickItem[] = [];
      for (let i = 1; i <= results.length; i++) {
        tables.push({
          label: 'Table ' + i,
          index: i - 1,
        });
      }

      let selected = await vscode.window.showQuickPick(tables);
      if (!selected) return;
      resultIndex = selected.index;
    }

    if (results[resultIndex].rowCount < 1) {
      vscode.window.showWarningMessage(
        'Unable to save data - table has no data',
      );
      return;
    }

    let formats = ['json', 'xml', 'csv'];
    let selFormat = await vscode.window.showQuickPick(formats);
    if (!selFormat) return;

    let fileData: string = null;
    if (selFormat === 'json') {
      let data = transformResult(results[resultIndex], true);
      fileData = JSON.stringify(data, null, 2);
    } else if (selFormat === 'xml') {
      var ser = new EasyXml({
        singularize: true,
        rootElement: 'results',
        dateFormat: 'ISO',
        manifest: true,
      });
      let data = transformResult(results[resultIndex], true);
      fileData = ser.render(data);
    } else if (selFormat === 'csv') {
      let columns = transformColumns(results[resultIndex].fields);

      let csvError: any = false;
      fileData = await new Promise<string>((resolve) => {
        csv(
          results[resultIndex].rows,
          {
            header: true,
            columns: columns,
            cast: {
              boolean: (value: boolean): string => {
                return value ? 'true' : 'false';
              },
              date: (value: Date, context: any) => {
                if (context.header) return value as unknown as string;
                let fieldInfo = results[resultIndex].fields![context.index];
                return formatFieldValue(fieldInfo, value, true);
              },
            },
          },
          (err, output: string) => {
            if (err) {
              csvError = err;
              resolve('');
              return;
            }
            resolve(output);
          },
        );
      });
    }

    try {
      let doc: vscode.TextDocument = await vscode.workspace.openTextDocument({
        language: selFormat,
      });
      let editor: vscode.TextEditor = await vscode.window.showTextDocument(
        doc,
        1,
        false,
      );
      let result = await editor.edit((edit) =>
        edit.insert(new vscode.Position(0, 0), fileData),
      );
      if (!result)
        vscode.window.showErrorMessage(
          'Error occurred opening content in editor',
        );
    } catch (err) {
      vscode.window.showErrorMessage(err);
    }
  }
}

function transformResult(result: QueryResults, raw: boolean) {
  let columns = transformColumns(result.fields);
  return result.rows.map((row) => transformData(columns, row, raw));
}

function transformData(
  fields: MappedField[],
  row: RowRecord,
  raw: boolean,
): RowRecord {
  let newRow: RowRecord = {};
  fields.forEach((field) => {
    newRow[field.name] = formatFieldValue(field, row[field.index], raw);
  });
  return newRow;
}

function transformColumns(fields: QueryResults['fields']) {
  let transformedFields: MappedField[] = [];
  let fieldCounts: any = {};
  fields.forEach((field, idx) => {
    if (fieldCounts.hasOwnProperty(field.name)) {
      fieldCounts[field.name]++;
      transformedFields.push({
        ...field,
        name: field.name + '_' + fieldCounts[field.name],
        header: field.name,
        key: field.name,
        index: idx,
      });
    } else {
      fieldCounts[field.name] = 0;
      transformedFields.push({
        ...field,
        header: field.name,
        key: field.name,
        index: idx,
      });
    }
  });
  return transformedFields;
}

type MappedField = QueryResults['fields'][0] & {
  header: string;
  index: number;
  key: string;
};
type RowRecord = Record<string, any>;
