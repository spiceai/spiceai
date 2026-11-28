-- Documents for search.yaml. Every body has exactly 12 words, so BM25
-- ranks a term by how often it occurs: `fox` occurs i % 5 times, `dog`
-- (i / 5) % 4 times, and filler words fill the rest. Some bodies are NULL.
CREATE TABLE docs (id int PRIMARY KEY, body text, fox int, dog int);
INSERT INTO docs
SELECT i,
  CASE WHEN i % 17 = 0 THEN NULL ELSE
    trim(repeat('fox ', i % 5) || repeat('dog ', (i / 5) % 4)
      || repeat('stone ', 12 - i % 5 - (i / 5) % 4)) END,
  i % 5, (i / 5) % 4
FROM generate_series(1, 80) i;
