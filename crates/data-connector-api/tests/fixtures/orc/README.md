# Independently produced ORC fixtures

`TestOrcFile.test1.orc` is Apache ORC's Java-writer example of the same name
(`examples/TestOrcFile.test1.orc` in [apache/orc](https://github.com/apache/orc)),
Apache License 2.0. It is **not** written by the `orc-rust` encoder the listing
reader uses.

The file has two rows. Glue catalog tables stored as `OrcInputFormat` are
registered through the same listing `file_format: orc` path these bytes exercise.
