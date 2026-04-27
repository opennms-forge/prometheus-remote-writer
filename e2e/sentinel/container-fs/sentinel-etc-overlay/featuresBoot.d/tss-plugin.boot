# Additive Sentinel boot features for the spike.
#
# Karaf's featuresBoot.d/ semantics: each .boot file lists feature names
# (one per line, blank lines and `#` comments allowed). The container's
# default boot list stays in place; entries here are appended.
#
# WHY each feature is here:
#   sentinel-kafka              IPC fabric (sink/rpc/twin client + server).
#                               Provides MessageConsumerManager that
#                               sentinel-telemetry needs.
#   sentinel-telemetry          Telemetry-adapter pipeline. Pulls in
#                               sentinel-timeseries-api transitively, which
#                               is the OIA TSS SPI lookup our plugin
#                               registers against.
#   sentinel-jsonstore-postgres Provides JsonStore service. Required by
#                               config-dao.thresholding.impl and
#                               config-dao.poll-outages.impl. Also
#                               transitively pulls sentinel-timeseries-api.
#   sentinel-blobstore-noop     Provides BlobStore service (no-op impl —
#                               we don't actually need to persist blobs
#                               for the spike, just satisfy the binding).
#                               Required by collection.thresholding.impl.
#   prometheus-remote-writer    Our plugin.
#
# NB: sentinel-thresholding-service (transitive from sentinel-telemetry)
# already declares sentinel-config-dao-{thresholding,poll-outages}, so
# those don't need explicit entries here.

sentinel-kafka
sentinel-telemetry
sentinel-jsonstore-postgres
sentinel-blobstore-noop
prometheus-remote-writer
