{
  description = "lakecli client interface";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, naersk }:
   flake-utils.lib.eachDefaultSystem (system:
      let
        naersk-lib = pkgs.callPackage naersk {};
        pkgs = nixpkgs.legacyPackages.${system};
      in
        {
          packages = {
            lakecli = naersk-lib.buildPackage {
              src = ./src;
            };
          };
          devShell = pkgs.mkShell {
             packages = [
               pkgs.cargo
               pkgs.clippy
               pkgs.rustc
               pkgs.rustfmt
             ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
               pkgs.darwin.Security
               pkgs.iconv
             ];

             shellHook = ''
             alias rustdoc='xdg-open ${pkgs.rustc.doc}/share/doc/rust/html/index.html'
             '';
           };
        }
    );
}
