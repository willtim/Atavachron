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

    $ atavachron init -r file:///home/tim/test-repo
    Enter password:
    Re-enter password:
    Credentials cached at /home/tim/.cache/atavachron/%2Fhome%2Ftim%2Ftest-repo/credentials
    Repository created at file:///home/tim/test-repo

### Backing up

To backup the folder '/home/tim/Pictures/Wallpaper' to this repository, we would use:

    $ atavachron backup -r file:///home/tim/test-repo -d /home/tim/Pictures/Wallpaper
    Files: 36  |  Chunks: 37  |  In: 50 MB  |  Out (dedup): 50 MB  |  Out (stored):  50 MB  |  Rate: 37 MB/s  |  Errors: 0
    Wrote snapshot 107ee7fd

### Listing

To list all snapshots, we provide the command 'snapshots' together with the mandatory repository option '-r':

    $ atavachron snapshots -r file:///home/tim/test-repo
    107ee7fd | tim      | x1c      | /home/tim/Pictures/Wallpaper     | 2018-06-15 07:16 | 2018-06-15 07:16

To list all files within a snapshot, we provide the command 'list' together with a snapshot ID.  We only need to specify enough of the snapshot ID to avoid ambiguity:

    $ atavachron list 107 -r file:///home/tim/test-repo

### Restoring

Files from a snapshot can be selectively restored to a target directory on the local filesystem. For example:

    $ atavachron restore 107 -r file:///home/tim/test-repo -i '**/*' -d /home/tim/tmp

### Verifying

Verification is similar to a restore. It will download all chunks from a repository and decode them, using cryptographic authentication to guarantee the integrity of each chunk, but it will not reconstitute the associated files to disk. It is used to test that the integrity of the data in the remote repository.

    $ atavachron verify 107 -r file:///home/tim/test-repo

A much faster but obviously less thorough test, is to simply check the existence of all referenced chunks in the storage. This can be accomplished using the `chunks --check` command.

### Add an additional access key

To create an additional access key with its own password, use the following:

    $ atavachron keys --add karen -r file:///home/tim/test-repo

Note that currently keys can only be revoked by deleting them manually from the Store, e.g. by using the Amazon S3 dashboard.

### Pruning

Prune will delete snapshots using a supplied retention policy. For example:

    $ atavachron prune -r file:///home/tim/test-repo --keep-daily 5 --keep-monthly 3

Note that it is distinct backups that count, e.g. keep 5 days means the last 5 recent distinct days. Snapshots selected by previous rules are not considered by later rules.

Prune will result in some chunks being marked as *garbage*. These are only ever deleted by issuing a `chunks --delete-garbage` command provided that the configured garbage expiry time has been exceeded for each garbage chunk considered. The expiry time defaults to 30 days and is intended to reflect the maximum possible backup time. This optimistic concurrency scheme permits concurrent backups during a prune. Concurrent prunes may generate warnings (a snapshot already deleted, or a chunk already collected as garbage).

### Backing up to Amazon S3

To backup to Amazon S3, we provide a URL using an S3 protocol prefix to a regional endpoint and bucket:

    $ atavachron backup -r s3://s3-eu-west-2.amazonaws.com/<bucket-name> -d /home/tim/Pictures

A list of all the Amazon regional endpoints can be found [here](https://docs.aws.amazon.com/general/latest/gr/rande.html). Ideally, AWS credentials should be provided in an INI-file located at `~/.aws/credentials`, currently it is only the default profile that is used. The format of this file is documented [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Alternatively, the environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` can be used, these are documented [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).

### Getting help

A list of the available commands can be obtained by using the `--help` switch. The usage for a particular command can be obtained by invoking the command without any switches.

## Repository structure

The repository structure is common to all store back-ends. At the root of the repository there is an encrypted file 'atavachron-manifest' which contains metadata such as the keys necessary to encrypt/decrypt/hash all chunks and snapshots. The chunks and snapshots are stored under their associated folders using content-derived keys. The 'keys' folder contains named access keys, which can be used to obtain the manifest encryption key and thus access to the repository, when combined with a matching password. The 'garbage' folder contains chunks that are believed to be no longer referenced by any snapshots in the repository, they are not visible to de-duplication, but they are still available to the restore command.

    .
    ├── chunks
    │   ├── 0e
    │   │   └── 0e15f4d3b47f32000be57d392f22c607b9cd8a8f92ad0df68d604f5f8543a4ac
    │   └── 4f
    │       └── 4f7fbb0cbec15377529ce2dc0dd2e3e9ebbd645f4312681a2de81ddee8099dc4
    ├── keys
    │   └── default
    ├── atavachron-manifest
    ├── bin
    ├── garbage
    └── snapshots
        └── 83f992ba4df155eef874b4708799a1a03d0bd1954b25802ddeb497053a0cc745


The bin folder optionally contains Atavachron binaries that were used to generate successful snapshots. These binaries are stored using their hash codes which are then recorded in each snapshot they create. They are not encrypted or chunked, but instead compressed using the bzip2 file format. The command-line `bzip2` tool can thus be used to recover these binaries, should there be unforeseen compatibility problems. Atavachron, like most modern software, has many third-party dependencies and so this gives us additional peace-of-mind. Note that for this to be a useful feature, Atavachron needs to be built as a statically-linked binary.

## Configuration file and profiles

Atavachron can make use of an optional configuration file, which as well as allowing us to tweak various runtime parameters, also allows us to save and recall one or more sets of backup parameters as "profiles". The following is an example configuration file, with a single profile:

    { cachePath         = Default{} -- location of the files, chunks and repository credentials caches
    , taskThreads       = Default{} -- number of lightweight threads used to chunk and upload/download
    , taskBufferSize    = Default{} -- number of concurrent tasks processed by the threads
    , garbageExpiryDays = Default{} -- a garbage expiry time in days, greater than the longest likely backup
    , maxRetries        = 10        -- maximum number of retries before failure
    , backupBinary      = True      -- backup the atavachron binary
    , profiles =
       [ { name = "pictures"
         , location ="s3://s3-eu-west-2.amazonaws.com/tims-backup"
         , source = "/tank/backups/tim/Pictures"
         , include = []
         , exclude = ["**/*.mp4", "**/*.VID"]
         , pruning = Disabled{}
         }
       ]
    }

The default name and location on Linux is `~/.config/atavachron/atavachron.x`, but one can be instead specified using the `-c` switch.
Profiles can be selected using the `-p` switch, for example:

    $ atavachron backup -p pictures

Any additional file globs specified on the command line will be applied in addition to those specified in the profile.

## FAQ

### Why not just use DropBox?

The most important reason for me, which applies to most consumer cloud storage offerings, is the difficulty of integrating trustworthy client-side encryption. I also do not want bi-directional sync, which is a complex problem and difficult to get right (see [here](https://www.cis.upenn.edu/~bcpierce/papers/mysteriesofdropbox.pdf)). DropBox does offer online fine-grained incremental backup, sending changes to the cloud one file at a time. They also use de-duplication, but it is for their benefit only.
Atavachron is a different point in the design space. In order to handle potentially huge amounts of files and maximise upload performance, it packs them into chunks. This also has the advantage of hiding the file sizes from the remote repository, which makes the encryption more secure. Atavachron backups are forever incremental, but there is potentially more cost in terms of storage space for each backup performed (depending on the chunk size chosen).

### Why not use an existing backup program for Amazon S3?

They will be more appropriate in most cases. However, typically other backup programs are not *scalable*, which can be a problem for large amounts of data and machines with limited memory. Atavachron is GPL3 licensed and will also appeal to those who would rather hack and modify a Haskell program in preference to anything else (which includes me!).

### What exactly do you mean by scalable?

Atavachron is scalable because the problem size (i.e. the number of files to be backed up) is not limited by the available memory of the machine performing the backup. It should be possible to backup many terabytes of data using a few gigabytes of working memory at most (depending on the level of concurrency configured). That said, there are some minor limitations, for example we do need to realise all the file names of a particular directory in memory in order to sort them and diff them. However, this is unlikely to be an issue in practice.

### What exactly do you mean by fast?

I believe it should be fast enough. The primary goals are clarity and correctness of the implementation. We will almost certainly take a performance hit by being scalable, as we do not use in-memory data structures for holding chunk sets and file lists.
Atavachron can chunk, hash, compress and encrypt the entire Linux source code repository, as of May 2018, to an alternative local folder in about 30 seconds on my old Thinkpad. Subsequent backups containing a few changes take seconds. When backing up to a remote repository such as Amazon S3, I suspect the throughput will be limited more by the performance of the network and the remote server.

### How does de-duplication work?

Atavachron uses content-derived chunking (CDC) for de-duplication. A rolling hash with per-repository secret parameters is used to derive boundaries in the packed file data. The idea is that for many types of file change, for example inserting bytes at the beginning, it should not be necessary to re-write many chunks. The parameters are chosen to give the desired statistical distribution of chunk size.
The chunks are then hashed using a secret key; and then, only if necessary, compressed, encrypted and uploaded. The chunks are stored in the remote repository, using their hashes as file names. Any new chunk with a matching hash can be identified as a duplicate. This works for the entire repository across multiple machines and backups.

### How secure is it?

The highly regarded *Libsodium* provides the high-level APIs for use by cryptography non-experts such as myself. Atavachron hashes using HMAC-SHA512 and encrypts using an XSalsa20 stream cipher with Poly1305 MAC authentication. For passwords, Atavachron uses Scrypt to generate an *access key* from a password and random salt. This access key is used to encrypt the manifest key which unlocks the repository manifest and therefore all the data within the repository. The encrypted manifest key and password salt are written into the store under /keys/ using a label chosen by the user. This scheme supports multiple passwords to the repository and the possibility of revoking/changing a password.

### How does prune work without locks?

Concurrent backups work well without any locks, since we are only ever appending to the repository. Pruning snapshots does however present problems, since we will need to delete chunks potentially referenced by other snapshots, including snapshots that are not yet visible. In order to support eventually-consistent storage like Amazon S3, we cannot safely use locks and so instead rely on a two-staged approach to pruning. The actual 'prune' command deletes only snapshot files and moves any chunks no longer referenced by the remaining snapshots into a garbage folder (garbage collection). If there is a concurrent backup running, it's possible that some chunks which were *optimistically* marked as garbage, turn out to be referenced once the concurrent backup completes. Such chunks will be restored as part of the `chunks --delete-garbage` command, which runs a repair prior to actually deleting the garbage. This command permanently deletes any garbage that matches the configured garbage expiry time (which defaults to 30 days). The expiry time should be set to exceed the longest likely backup time and ensures that any concurrently running backup that started before the last prune operation will have finished.

### Why Haskell?

Haskell offers a level of type-safety and expressiveness that is unmatched by most other practical languages. GHC Haskell is also capable of producing highly performant executables from very high-level abstract code. Atavachron has been written largely by composing transformations on effectful on-demand streams, resulting in better modularity and separation-of-concerns when compared to more traditional approaches. The high-level pipeline architecture should be visible in the source file [Pipelines.hs](https://github.com/willtim/Atavachron/blob/master/src/Atavachron/Pipelines.hs).

### What is property-based testing?

Property-based tests consist of assertions and logical properties that a function should fulfil. These properties are tested for many different randomly-generated function inputs, essentially generating test cases automatically. The canonical property-based testing framework is Haskell's QuickCheck.

### Will there be native Windows support?

This isn't currently planned. However, I do intend to test it under the Windows 10 Subsystem for Linux (WSL).

### Where does the name come from?

The Atavachron was the time machine that Kirk, Spock and McCoy unwittingly used in the Star Trek episode "All our Yesterdays". Atavachron is also a really good 80's jazz fusion album by the late great guitarist Allan Holdsworth.
