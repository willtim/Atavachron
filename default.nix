{ mkDerivation, async, attoparsec, aws, base, base16-bytestring
, binary, bytestring, conduit, conduit-extra, containers
, direct-sqlite, directory, exceptions, expresso, fast-logger
, filepath, Glob, hashable, hostname, http-client, http-conduit
, http-types, ini, lens, lz4, mmorph, mtl, optparse-applicative
, parsec, QuickCheck, random, resourcet, saltine, scrypt, serialise
, stdenv, streaming, tasty-quickcheck, template-haskell, text, time
, time-locale-compat, transformers, transformers-base, unix
, unordered-containers, uri-encode, vector, wl-pprint
}:
mkDerivation {
  pname = "atavachron";
  version = "0.2.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    async attoparsec aws base base16-bytestring binary bytestring
    conduit conduit-extra containers direct-sqlite directory exceptions
    fast-logger filepath Glob hashable hostname http-client
    http-conduit http-types ini lens lz4 mmorph mtl
    optparse-applicative parsec QuickCheck random resourcet saltine
    scrypt serialise streaming tasty-quickcheck template-haskell text
    time time-locale-compat transformers transformers-base unix
    unordered-containers uri-encode vector wl-pprint
  ];
  executableHaskellDepends = [
    async attoparsec aws base base16-bytestring binary bytestring
    conduit conduit-extra containers direct-sqlite directory exceptions
    fast-logger filepath Glob hashable hostname http-client
    http-conduit http-types ini lens lz4 mmorph mtl
    optparse-applicative parsec QuickCheck random resourcet saltine
    scrypt serialise streaming tasty-quickcheck template-haskell text
    time time-locale-compat transformers transformers-base unix
    unordered-containers uri-encode vector wl-pprint
  ];
  description = "Fast, scalable and secure de-duplicating backup";
  license = stdenv.lib.licenses.gpl3;
}
