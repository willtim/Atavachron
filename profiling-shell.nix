# INSTRUCTIONS:
# - Add -prof and -fprof-auto to our benchmark's GHC options
# - Regenerate default.nix
# - Enter this profiling shell: nix-shell profiling-shell.nix
# - cabal configure --enable-library-profiling --enable-benchmarks
# - cabal build
# - dist/build/atavachron/atavachron-bench +RTS -p
# - Look at the produced atavachron-bench.prof file

let nixpkgs = import <nixpkgs> {};
    orig = nixpkgs.pkgs.profiledHaskellPackages.callPackage ./default.nix {};
in (nixpkgs.pkgs.haskell.lib.doBenchmark orig).env
