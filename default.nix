{ mkDerivation, async, attoparsec, aws, base, base16-bytestring
, binary, bytestring, bzlib, conduit, conduit-extra, containers
, directory, exceptions, expresso, filepath, Glob
, hashable, hostname, http-client, http-conduit, http-types, ini
, lens, logging, lz4, mmorph, mtl, optparse-applicative, parsec
, QuickCheck, random, resourcet, saltine, scrypt, serialise, stdenv
, streaming, tasty-quickcheck, template-haskell, text, time
, transformers, transformers-base, unix, unordered-containers
, uri-encode, vector, wl-pprint
}:
mkDerivation {
  pname = "atavachron";
  version = "0.1.2.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    async attoparsec aws base base16-bytestring binary bytestring bzlib
    conduit conduit-extra containers directory exceptions
    expresso filepath Glob hashable hostname http-client http-conduit
    http-types ini lens logging lz4 mmorph mtl optparse-applicative
    parsec QuickCheck random resourcet saltine scrypt serialise
    streaming tasty-quickcheck template-haskell text time transformers
    transformers-base unix unordered-containers uri-encode vector
    wl-pprint
  ];
  executableHaskellDepends = [
    async attoparsec aws base base16-bytestring binary bytestring bzlib
    conduit conduit-extra containers directory exceptions
    expresso filepath Glob hashable hostname http-client http-conduit
    http-types ini lens logging lz4 mmorph mtl optparse-applicative
    parsec QuickCheck random resourcet saltine scrypt serialise
    streaming tasty-quickcheck template-haskell text time transformers
    transformers-base unix unordered-containers uri-encode vector
    wl-pprint
  ];
  description = "Fast, scalable and secure de-duplicating backup";
  license = stdenv.lib.licenses.gpl3;
}
