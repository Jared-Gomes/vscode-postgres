This fork of the vscode extension `vscode-postgres` adds support for projects using the `postgres` npm package referred to as Postgres.js.

It adds:

- query detection in typescript files using the regex ``/(sql|authSql|tx|authTx)(?:<([^>]+)>)?`([\s\S]*?)`/g``
- support for multiple queries in a typescript file
- syntax highlighting for query strings
- parsing of Postgres.js tagged template literals
- in-line error detection for query strings
