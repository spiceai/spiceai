# cloud-connect-crypto

The sealed-secret wire crypto for Spice Cloud Connect: the HPKE suite, `key_id`
derivation, both AAD forms and the canonicalisation of their inputs, the
recipient keypair and its encodings, `seal`, and `open`.

More than one component has to produce these bytes identically — the sealer of
each of the two layers, and the recipient that opens both. This crate is the
single Rust source for all of them, and the normative reference for
implementations that are not Rust.

## Why it is a crate and not a file each side copies

When each component keeps its own copy of ~20 lines of byte concatenation, a
component that starts canonicalising one field differently produces ciphertext
that opens nowhere. HPKE reports only that authentication failed: nothing in the
failure points at the field that diverged, and the payload fails closed at the
far end with no diagnosable cause. That has happened more than once, each time
on a different field, and each time it took a cross-component audit to find.

Stating the rule in prose closes the class by convention. A shared crate closes
it by construction, which is the difference between a rule several teams have to
remember and a rule the compiler enforces.

[`SecretAddress`](src/aad.rs) is where that happens: it is the only way to reach
either AAD, it canonicalises on the way in, and both forms come off the same
canonical value, so the two layers of one payload cannot disagree about a
component.

## The contract

```text
suite   HPKE (RFC 9180) base mode, DHKEM(X25519, HKDF-SHA256) / HKDF-SHA256 / ChaCha20-Poly1305
info    spice-cloud-connect/secrets/v1
key_id  hex(SHA-256(raw KEM public key)[..8])

outer AAD   external_id 0x00 namespace 0x00 secret_name 0x00 key_id 0x00 command_id
inner AAD   external_id 0x00 namespace 0x00 secret_name 0x00 key_id
```

Canonicalisation: `namespace` and `secret_name` are trimmed exactly once, on
ingest; `external_id`, `key_id`, and `command_id` are verbatim; nothing is
Unicode-normalised or case-folded; no component may contain the separator byte.
**Adding a component means deciding its rule first and implementing it
everywhere in one change.**

Trimming means Rust's `str::trim` — leading and trailing characters with the
Unicode `White_Space` property. That is not what every language's `trim` does:
JavaScript's also removes `U+FEFF`, which `White_Space` does not include. An
implementation in another language has to match this rule rather than reach for
its own trim.

## Using it from another repository

```toml
[dependencies]
cloud-connect-crypto = { git = "https://github.com/spiceai/spiceai.git", rev = "<full 40-char sha>" }
```

Pin a full 40-character SHA — reproducible and unambiguous. Bump it in the same
review as any change to the contract.

Nothing in the crate is environment-specific: it knows nothing about Kubernetes,
transport, storage, or the generated proto types, so a caller adapts its own
`SecretsKey` / `ApplySecrets` message types at the boundary and keeps every byte
decision here.

### Replacing a local copy

The crate produces the same bytes as the copies it replaces, but the API is not
identical, and one size cap changed value. Three differences are worth knowing
before the swap:

- **There are no free AAD functions.** Both forms come off a `SecretAddress`,
  which is built from the message fields once and canonicalises them. A sealer
  that trims on ingest keeps doing so — trimming is idempotent, and the values
  it puts on the wire should be `address.namespace()` / `address.secret_name()`.
  For the two layers of one payload, build the address once and re-point it at
  the outer recipient with `with_key_id`.
- **`seal` takes a `SealLayer`, and the outer layer's cap is larger than the
  inner one's.** The two layers seal different things: the inner seals the secret
  payload, the outer seals the serialized envelope wrapping it. That envelope is
  *always* bigger than the secret inside it — an encapsulated key, an AEAD tag,
  and its own framing — so holding the outer layer to the secret's 1 MiB limit
  makes a secret at exactly that limit sealable once and then impossible to wrap.
  `SealLayer::Outer` is bounded instead so its ciphertext still fits a
  recipient's arrival cap. A copy that capped both layers at 1 MiB has this bug;
  the fix widens what is accepted, so nothing that worked before stops working.
- **The two size caps are named apart.** `MAX_SECRET_PLAINTEXT_SIZE` (1 MiB) is
  the Kubernetes `Secret` limit and caps the inner payload;
  `MAX_SEALED_SECRETS_SIZE` (1 MiB + 1 KiB) caps a sealed blob as it *arrives*
  and is enforced inside `open`, so an oversized ciphertext is refused on its
  length before anything is decrypted rather than after the AEAD tag at the end
  of it fails.

Smaller ones: failures are a typed `Error` rather than a `String`, `open` and
`seal` are on the key types, `to_pkcs8_pem` returns a `Zeroizing<String>` (build
a `SecretString` from `as_str()` if that is what a caller stores), and an
announced key is validated by `RecipientKey::from_announcement`, which takes the
announcement's fields rather than a generated proto type — adapt at the
boundary.

## For an implementation that is not Rust

`testdata/conformance_vectors.json` is a language-neutral artifact — suite
parameters, the canonicalisation rule, `key_id` derivations, both AAD forms over
inputs chosen to break a naive implementation, and the inputs that must be
*rejected* — with all bytes hex-encoded. Read it, apply your implementation to
each `input`, and assert byte equality against `canonical`, `inner_aad_hex`,
`outer_aad_hex`, and `key_id`. A divergence then fails your CI instead of
surfacing as an unexplained open failure in a customer's cluster.

Vectors worth reading before writing the code: `bom_is_not_whitespace`,
`verbatim_components_keep_their_padding`, `empty_command_id`, and the
`unicode_nfc` / `unicode_nfd` pair.

The artifact carries no ciphertext. HPKE encapsulates a fresh key per seal, so a
ciphertext is not reproducible and pinning one would prove nothing; every
component-level interop fixture that does pin one stays where it is, unchanged.

Regenerate with:

```sh
UPDATE_CLOUD_CONNECT_VECTORS=1 cargo test -p cloud-connect-crypto
```

Regeneration is meant to be deliberate and reviewable. The committed file moving
in a diff is the signal that the contract moved and that every other
implementation has to move with it in the same change.

## Changing anything here

Every constant, every join, and the canonicalisation rule are wire contract. A
change to any of them makes ciphertext already in flight un-openable, so it is
not a refactor: it needs a new `info` label and a rollout that covers both, not
an edit to the existing one.
