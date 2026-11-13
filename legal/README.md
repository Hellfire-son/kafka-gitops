<!-- SPDX-License-Identifier: Apache-2.0 -->
# Redistributed component licenses

`third-party/` contains unchanged license/notice texts from the exact runtime
JARs, source JARs, tagged official source archives and native subcomponents.
`runtime-components.json` records the reviewed coordinates, runtime bytes,
source origins/checksums, selected licenses and SHA-256 of every included text.
`SOURCE-HEADERS.txt` files preserve original header credits from source files;
the extraction headings identify them and do not modify the comment bodies.

Logback's EPL-2.0 option is selected. Each module has the original dual-license
notice, full EPL text and source-availability statement for the exact version.
Zstandard's BSD option is selected. The alternative GPL text is retained only
as an upstream legal resource. Native GCC runtime texts include the Runtime
Library Exception; their presence does not relicense the bindings under GPL.
The GCC 15.2.0 source mirror supplies the fixed exception text, not proof that
all bundled native binaries were built with that compiler version.

LZ4's exact submodule commit is recorded. Snappy's source VERSION file identifies
Snappy 1.1.10 and Bitshuffle 0.3.4. The binding's LICENSE/NOTICE, Snappy COPYING,
Bitshuffle MIT and additional upstream native notices in separate source directories
are retained. Java POM license metadata alone was not used to infer native terms.

`prepareLegalBundle` copies the reviewed texts into unique component paths in
the JAR, together with the original project LICENSE, provenance and a combined
notice index. Shared legacy license paths may have collisions in upstream JARs;
their exact original bytes survive under each component's `upstream-jar/` path.
Service descriptors are merged independently by Shadow.

Git attributes preserve original third-party legal bytes on every checkout.
The project LICENSE is unchanged in the source tree; its packaged copy uses LF
line endings for Windows/Linux consistency. This does not alter the license text.
Packaged provenance and release notes also use LF, independently of worktree line
endings. Original third-party legal resources are copied without normalization.

`verifyReleaseArtifact` fails when a runtime coordinate/JAR checksum changes,
when a reviewed legal text is missing/changed, or when an original input JAR
legal resource is lost. It also verifies services/JNI/bytecode/manifest.
No license text is fetched during a normal build. Dependency updates require
explicitly retrieving and reviewing the exact new official sources and updating
the manifest; no automatic license substitution or blanket Apache relabeling.

This is a documented distribution choice and technical preservation check,
not a jurisdiction-wide legal certification or a complete binary/toolchain SBOM.
The project LICENSE and upstream copyrights remain unchanged. Build/test tools
and container/JRE/OS distributions have separate redistribution obligations.
