# Current-prefix retained attachment boundary

The upstream runner's reuse fingerprint covers session resources, agent skills,
and environment config. It does not contain `Session.metadata`. The retained
fingerprint likewise keeps the current prefix hash/cutoff out of VM identity.
Static runtime code, scope, environment name/config, allocation (including exact
host/guest paths and read-only flags), resources/skills and DB target/role remain
bound; ordinary configuration mismatch is still refused.

`validateReplayAttachment` reuses the prepared adapter's public
`verifyReplayEvidence` export. On every warm bind with a known allocation, and
before initial retained allocation, it verifies the latest native session's
run/hash/cutoff against the actual currently published manifest and files. It
then requires exactly one `/replay/evidence` mount, read-only, whose host path
matches that freshly verified run. Changing metadata to a different valid run
cannot silently switch or retain the wrong mount. Mutable metadata is validation
input, not a new allocation fingerprint.

The verifier and its public index source hashes are captured in runtime identity.
An incomplete publication, unlisted staging file, stale metadata/hash, missing
metadata, changed run or mount fails closed. No older prefix, empty view or
replacement sandbox is substituted.

The Python `ReplayEvidenceView` publishes content-addressed files and replaces
the manifest within one stable run directory; it does not rename the mounted
root directory. The Node24 filesystem integration verifies that root inode stays
stable. The same static mount remains bound across a current-prefix advance and
controller replacement. Actual guest visibility and cache behavior of the native
mounted directory remain unproved until the coordinated native run; host
filesystem or synthetic native tests do not certify that behavior.

A controller restart is intended to reload the same logical session's current
metadata and canonical retained runtime ID, preserving that session's workspace.
The offline test exercises actual SQL/provider/adapter and current-prefix verifier
with a synthetic native client/disk, confirming the same ID/workspace boundary.
It does not certify actual OpenMA API history or real VM durability. Those proofs
remain required before acceptance.

Host replay CLI process resume is not implemented or claimed. A new CLI run uses
a fresh isolated repository/run, and incomplete publication is refused. No SQL
unarchive, replacement live session, archive-on-idle, synthetic calibration
state, model call or new service is introduced by this attachment fix.

The prepared native qualifier separately requires final CPU1/memory2048 in a
fresh dedicated scope/home. Historical512MiB records are not relabeled or restored
into that2048MiB scope. No native run/build is authorized by this document.
