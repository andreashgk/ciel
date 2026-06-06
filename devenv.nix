{ pkgs, ... }:
{
  languages.rust = {
    enable = true;
    channel = "nightly";
    mold.enable = true;
    targets = [
      "x86_64-unknown-linux-musl"
    ];
  };

  packages = with pkgs; [
    buildah
  ];

  scripts.build-oci-image.exec = ''
    set -e

    cargo build --release --target x86_64-unknown-linux-musl

    container=$(buildah from docker.io/library/alpine:3.23)

    buildah run $container -- apk add --no-cache openssh-client

    buildah copy $container \
      ./target/x86_64-unknown-linux-musl/release/ciel \
      /usr/local/bin/ciel

    # Override alpine's default `cmd` parameter.
    buildah config --cmd "" $container
    buildah config --entrypoint '["/usr/local/bin/ciel"]' $container

    buildah commit $container ciel:dev
    buildah rm $container
  '';
}
