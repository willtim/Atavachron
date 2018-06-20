# ✇ Atavachron

## Fast, scalable and secure de-duplicating backup

- Scalable to any repository size (stream-based architecture)
- Verifiable immutable snapshots that are forever incremental
- Content-derived chunking for optimal de-duplication
- Encryption (libsodium) and compression (LZ4)
- Lock-free repository sharing and de-duplication across multiple machines
- Multi-threaded chunk processing and upload
- Property-based testing of core processing pipeline
- Amazon S3 support

*WARNING: Currently under active development and not yet ready for widespread use*

## Quick start

### Building

Atavachron is a Haskell cabal project. It should be buildable by installing the latest GHC Haskell and invoking 'cabal install' from the git repository. For Nix users, there is a 'shell.nix' file included.

### Initialising a repository

To test atavachron, we can initialise a local filesystem repository:

    $ atavachron init -r file:/home/tim/test-repo
    Enter password:
    Re-enter password:
    Credentials cached at /home/tim/.cache/atavachron/%2Fhome%2Ftim%2Ftest-repo/credentials
    Repository created at file:/home/tim/test-repo

### Backing up

To backup the folder '/home/tim/Pictures/Wallpaper' to this repository, we would use:

    $ atavachron backup -r file:/home/tim/test-repo -d /home/tim/Pictures/Wallpaper
    Files: 36  |  Chunks: 40  |  Input: 53 MB  |  Output: 53 MB  |  Errors: 0
    Wrote snapshot 107ee7fd

### Listing

To list all snapshots, we provide the command 'list' together with the '--snapshots' flag:

    $ atavachron list --snapshots -r file:/home/tim/test-repo -d /home/tim/Pictures/Wallpaper
    107ee7fd | tim      | x1c      | /home/tim/Pictures/Wallpaper     | 2018-06-15 07:16 | 2018-06-15 07:16

To list all files within a snapshot, we provide the command 'list' together with a snapshot ID.  We only need to specify enough of the snapshot ID to avoid ambiguity:

    $ atavachron list 107 -r file:/home/tim/test-repo

### Restoring

Files from a snapshot can be selectively restored to a target directory on the local filesystem. For example:

    $ atavachron restore 107 -r file:/home/tim/test-repo -i '**/*' -d /home/tim/tmp

### Verifying

Verification is similar to a restore. It will download all chunks from a repository and decode them, using cryptographic authentication to guarantee the integrity of each chunk, but it will not reconstitute the associated files to disk. It is used to test that the integrity of the data in the remote repository.

    $ atavachron verify 107 -r file:/home/tim/test-repo

### Backing up to Amazon S3

To backup to Amazon S3, we provide a URL using an S3 protocol prefix to a regional hostname and bucket. A list of all the Amazon regional endpoints can be found [here](https://docs.aws.amazon.com/general/latest/gr/rande.html). Currently the credentials must be provided in an ".s3cfg" configuration file.

    $ atavachron backup -r s3:s3-eu-west-2.amazonaws.com/<bucket-name> -d /home/tim/Pictures


## Repository structure

The repository structure is common to all store back-ends. At the root of the repository there is an encrypted file 'manifest' which contains metadata such as the keys necessary to encrypt/decrypt/hash all chunks and snapshots. The chunks and snapshots are stored under their associated folders using content-derived keys. The 'keys' folder contains named access keys, which can be used to obtain the manifest encryption key and thus access to the repository, when combined with a matching password.

    .
    ├── chunks
    │   ├── 0e
    │   │   └── 0e15f4d3b47f32000be57d392f22c607b9cd8a8f92ad0df68d604f5f8543a4ac
    │   └── 4f
    │       └── 4f7fbb0cbec15377529ce2dc0dd2e3e9ebbd645f4312681a2de81ddee8099dc4
    ├── keys
    │   └── default
    ├── manifest
    └── snapshots
        └── 83f992ba4df155eef874b4708799a1a03d0bd1954b25802ddeb497053a0cc745


## FAQ

### Why not just use DropBox?

The most important reason for me, which applies to most consumer cloud storage offerings, is the difficulty of integrating trustworthy client-side encryption. I also do not want bi-directional sync, which is a complex problem and difficult to get right. DropBox does offer online fine-grained incremental backup, sending changes to the cloud one file at a time. They also use de-duplication, but it is for their benefit only.
Atavachron is a different point in the design space. In order to handle potentially huge amounts of files and maximise upload performance, it packs them into chunks. This also has the advantage of hiding the file sizes from the remote repository, which makes the encryption more secure. Atavachron backups are forever incremental, but there is potentially more cost in terms of storage space for each backup performed (depending on the chunk size chosen).

### Why not use an existing backup program for Amazon S3?

They will be more appropriate in most cases. However, typically other backup programs are not *scalable*, which can be a problem for large amounts of data and machines with limited memory. Atavachron is GPL3 licensed and will also appeal to those who would rather hack and modify a Haskell program in preference to anything else (which include me!).

### What exactly do you mean by scalable?

Atavachron is scalable because the problem size (i.e. the number of files to be backed up) is not limited by the available memory of the machine performing the backup. It should be possible to backup terabytes of data using only a few hundred megabytes of working memory. That said, there are some minor limitations, for example we do need to realise all the file names of a particular directory in memory in order to sort them and diff them. However, this is unlikely to be an issue in practice.

### What exactly do you mean by fast?

I believe it should be fast enough. The primary goals are clarity and correctness of the implementation. We will almost certainly take a performance hit by being scalable, as we do not use in-memory data structures for holding chunk sets and file lists.
Atavachron can chunk, hash, compress and encrypt the entire Linux source code repository, as of May 2018, to an alternative local folder in about 30 seconds on my old Thinkpad. Subsequent backups containing a few changes take seconds. When backing up to a remote repository such as Amazon S3, I suspect the throughput will be limited more by the performance of the network and the remote server.

### How does de-duplication work?

Atavachron uses content-derived chunking (CDC) for de-duplication. A rolling hash with per-repository secret parameters is used to derive boundaries in the packed file data. The idea is that for many types of file change, for example inserting bytes at the beginning, it should not be necessary to re-write many chunks. The parameters are chosen to give the desired statistical distribution of chunk size.
The chunks are then hashed using a secret key; and then, only if necessary, compressed, encrypted and uploaded. The chunks are stored in the remote repository, using their hashes as file names. Any new chunk with a matching hash can be identified as a duplicate. This works for the entire repository across multiple machines and backups.

### How secure is it?

The highly regarded *Libsodium* provides the high-level APIs for use by cryptography non-experts such as myself. Atavachron hashes using HMAC-SHA512 and encrypts using an XSalsa20 stream cipher with Poly1305 MAC authentication. For passwords, Atavachron uses Scrypt to generate an *access key* from a password and random salt. This access key is used to encrypt the manifest key which unlocks the repository manifest and therefore all the data within the repository. The encrypted manifest key and password salt are written into the store under /keys/ using a label chosen by the user. This scheme supports multiple passwords to the repository and the possibility of revoking/changing a password.

### Why Haskell?

Haskell offers a level of type-safety and expressiveness that is unmatched by most other practical languages. GHC Haskell is also capable of producing highly performant executables from very high-level abstract code. Atavachron has been written largely by composing transformations on effectful on-demand streams, resulting in better modularity and separation-of-concerns when compared to more traditional approaches. The high-level pipeline architecture should be visible in the source file [Pipelines.hs](https://github.com/willtim/Atavachron/blob/master/src/Atavachron/Pipelines.hs).

### What is property-based testing?

Property-based tests consist of assertions and logical properties that a function should fulfil. These properties are tested for many different randomly-generated function inputs, essentially generating test cases automatically. The canonical property-based testing framework is Haskell's QuickCheck.

### Will there be native Windows support?

This isn't currently planned. However, I do intend to test it under the Windows 10 Subsystem for Linux (WSL).

### Where does the name come from?

The Atavachron was the time machine that Kirk, Spock and McCoy unwittingly used in the Star Trek episode "All our Yesterdays". Atavachron is also a really good 80's jazz fusion album by the late great guitarist Allan Holdsworth.
