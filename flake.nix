{
  description = "Spice. A portable runtime offering developers a unified SQL interface to materialize, accelerate, and query data";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixpkgs-unstable";
  };

  outputs = {
    self,
    nixpkgs,
    ...
  }: let
    supportedSystems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    forEachSupportedSystem = f:
      nixpkgs.lib.genAttrs supportedSystems (system:
        f {
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              self.overlay
            ];
          };
        });
  in {
    packages = forEachSupportedSystem ({pkgs, ...}: rec {
      spice = pkgs.buildGoModule {
        name = "spice";
        src = ./.;
        vendorHash = "sha256-9bjpd73NyNVfOFgoad27MElgTLZl2P//iUdwUn70ypc=";
        runVend = true;
      };
      default = spice;
    });

    formatter = forEachSupportedSystem ({pkgs, ...}: pkgs.alejandra);

    devShells = forEachSupportedSystem ({pkgs, ...}: {
      default = pkgs.mkShell {
        buildInputs = with pkgs; [
          go
          gopls
        ];

        packages = [];
      };
    });

    overlay = final: prev: {
      spice-pkgs = self.packages.${prev.system};
    };
  };
}
