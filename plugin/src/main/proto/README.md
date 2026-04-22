# Vendored Prometheus Remote Write v1 protobuf definitions

The two `.proto` files in this directory are vendored from the Prometheus
project, release `release-2.53`:

- `types.proto` — https://github.com/prometheus/prometheus/blob/release-2.53/prompb/types.proto
- `remote.proto` — https://github.com/prometheus/prometheus/blob/release-2.53/prompb/remote.proto

Both are Apache License 2.0. We have applied three local modifications:

1. Removed `import "gogoproto/gogo.proto";` lines — the [gogoprotobuf](https://github.com/gogo/protobuf)
   extensions are a Go-specific plugin and are not consumable by the Java
   `protoc` toolchain. The Java generated code is unaffected by their absence.
2. Stripped every `[(gogoproto.*) = *]` field annotation for the same reason.
3. Added two Java-specific options so the generated classes land under this
   plugin's namespace:
   ```
   option java_package = "org.opennms.plugins.prometheus.remotewriter.wire.proto";
   option java_multiple_files = true;
   ```

No other changes. The semantic protobuf wire format is byte-for-byte
compatible with upstream Prometheus Remote Write v1.

When the Prometheus project updates its Remote Write v1 definitions in a
compatible way, refresh by re-running the vendoring script from the commit
message that introduced these files and re-applying the three edits above.
