use anyhow::{bail, Result};
use dsi_progress_logger::ProgressLogger;
use log::info;
use mmap_rs::Mmap;
use std::io::prelude::*;
use webgraph::prelude::*;

pub struct SortId(usize);

#[repr(u8)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Object type of an SWHID
///
/// # Reference
/// - <https://docs.softwareheritage.org/devel/swh-model/data-model.html>
pub enum SWHType {
    Content = 0,
    /// a list of named directory entries, each of which pointing to other
    /// artifacts, usually file contents or sub-directories. Directory entries
    /// are also associated to some metadata stored as permission bits.
    Directory = 1,
    /// code “hosting places” as previously described are usually large
    /// platforms that host several unrelated software projects. For software
    /// provenance purposes it is important to be more specific than that.
    ///
    /// Software origins are fine grained references to where source code
    /// artifacts archived by Software Heritage have been retrieved from. They
    /// take the form of `(type, url)` pairs, where url is a canonical URL
    /// (e.g., the address at which one can `git clone` a repository or download
    /// a source tarball) and `type` the kind of software origin (e.g., git,
    /// svn, or dsc for Debian source packages).
    Origin = 2,
    ///AKA “tags”
    ///
    /// some revisions are more equals than others and get selected by
    /// developers as denoting important project milestones known as “releases”.
    /// Each release points to the last commit in project history corresponding
    /// to the release and carries metadata: release name and version, release
    /// message, cryptographic signatures, etc.
    Release = 3,
    /// AKA commits
    ///
    /// Software development within a specific project is
    /// essentially a time-indexed series of copies of a single “root” directory
    /// that contains the entire project source code. Software evolves when a d
    /// eveloper modifies the content of one or more files in that directory
    /// and record their changes.
    ///
    /// Each recorded copy of the root directory is known as a “revision”. It
    /// points to a fully-determined directory and is equipped with arbitrary
    /// metadata. Some of those are added manually by the developer
    /// (e.g., commit message), others are automatically synthesized
    /// (timestamps, preceding commit(s), etc).
    Revision = 4,
    /// any kind of software origin offers multiple pointers to the “current”
    /// state of a development project. In the case of VCS this is reflected by
    /// branches (e.g., master, development, but also so called feature branches
    /// dedicated to extending the software in a specific direction); in the
    /// case of package distributions by notions such as suites that correspond
    /// to different maturity levels of individual packages (e.g., stable,
    /// development, etc.).
    ///
    /// A “snapshot” of a given software origin records all entry points found
    /// there and where each of them was pointing at the time. For example, a
    /// snapshot object might track the commit where the master branch was
    /// pointing to at any given time, as well as the most recent release of a
    /// given package in the stable suite of a FOSS distribution.
    Snapshot = 5,
}

impl TryFrom<u8> for SWHType {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::Content,
            1 => Self::Directory,
            2 => Self::Origin,
            3 => Self::Release,
            4 => Self::Revision,
            5 => Self::Snapshot,
            _ => bail!("Invalid SWHType {}.", value),
        })
    }
}

impl SWHType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Content => "cnt",
            Self::Directory => "dir",
            Self::Origin => "ori",
            Self::Release => "rel",
            Self::Revision => "rev",
            Self::Snapshot => "snp",
        }
    }
}

impl core::fmt::Display for SWHType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
/// SoftWare Heritage persistent IDentifiers
///
/// A SWHID consists of two separate parts, a mandatory core identifier that
/// can point to any software artifact (or “object”) available in the Software
/// Heritage archive, and an optional list of qualifiers that allows to specify
/// the context where the object is meant to be seen and point to a subpart of
/// the object itself.
///
/// # Reference
/// - <https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html>
/// - Roberto Di Cosmo, Morane Gruenpeter, Stefano Zacchiroli. [Identifiers for Digital Objects: the Case of Software Source Code Preservation](https://hal.archives-ouvertes.fr/hal-01865790v4). In Proceedings of iPRES 2018: 15th International Conference on Digital Preservation, Boston, MA, USA, September 2018, 9 pages.
/// - Roberto Di Cosmo, Morane Gruenpeter, Stefano Zacchiroli. [Referencing Source Code Artifacts: a Separate Concern in Software Citation](https://arxiv.org/abs/2001.08647). In Computing in Science and Engineering, volume 22, issue 2, pages 33-43. ISSN 1521-9615, IEEE. March 2020.
pub struct SWHID {
    /// Namespace Version
    pub namespace_version: u8,
    /// Node type
    pub node_type: SWHType,
    /// SHA1 has of the node
    pub hash: [u8; 20],
}

impl SWHID {
    pub const BYTES_SIZE: usize = 2;
}

impl core::fmt::Display for SWHID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "swh:{}:{}:",
            self.namespace_version,
            self.node_type.to_str(),
        )?;
        for byte in self.hash.iter() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl TryFrom<[u8; SWHID::BYTES_SIZE]> for SWHID {
    type Error = anyhow::Error;
    fn try_from(value: [u8; SWHID::BYTES_SIZE]) -> std::result::Result<Self, Self::Error> {
        let namespace_version = value[0];
        if namespace_version != 1 {}
        let node_type = SWHType::try_from(value[1])?;
        let mut hash = [0; 20];
        hash.copy_from_slice(&value[2..]);
        Ok(Self {
            namespace_version,
            node_type,
            hash,
        })
    }
}

impl From<SWHID> for [u8; SWHID::BYTES_SIZE] {
    fn from(value: SWHID) -> Self {
        let mut result = [0; SWHID::BYTES_SIZE];
        result[0] = value.namespace_version;
        result[1] = value.node_type as u8;
        result[2..].copy_from_slice(&value.hash);
        result
    }
}

/// Struct to load a `.node2swhid.bin` file and convert node ids to SWHIDs.
pub struct Node2SWHID {
    data: Mmap,
}

impl Node2SWHID {
    /// Load a `.node2swhid.bin` file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(file, 0)
                .map()?
        };
        Ok(Self { data })
    }
}

impl Node2SWHID {
    /// Convert a node_it to a SWHID
    pub fn get(&self, node_id: SortId) -> Option<SWHID> {
        let offset = node_id.0 * SWHID::BYTES_SIZE;
        let bytes = self.data.get(offset..offset + SWHID::BYTES_SIZE)?;
        // this unwrap is always safe because we use the same const
        let bytes: [u8; SWHID::BYTES_SIZE] = bytes.try_into().unwrap();
        // this unwrap can only fail on a corrupted file, so it's ok to panic
        Some(SWHID::try_from(bytes).unwrap())
    }

    /// Return how many node_ids are in this map
    pub fn len(&self) -> usize {
        self.data.len() / SWHID::BYTES_SIZE
    }
}

impl core::ops::Index<SortId> for Node2SWHID {
    type Output = SWHID;
    fn index(&self, index: SortId) -> &Self::Output {
        let offset = index.0 * SWHID::BYTES_SIZE;
        let bytes = &self.data[offset..offset + SWHID::BYTES_SIZE];
        debug_assert!(core::mem::size_of::<SWHID>() == SWHID::BYTES_SIZE);
        // unsafe :( but it's ok because SWHID does not depends on endianess
        // also TODO!: check for version
        unsafe { &*(bytes.as_ptr() as *const SWHID) }
    }
}

/// A struct that stores the mapping between the node ids in the graph
/// and the node_ids before compression
pub struct Order {
    data: Mmap,
}

impl Order {
    /// Load a `.order` file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(file, 0)
                .map()?
        };
        Ok(Self { data })
    }
}

impl Order {
    /// Convert an in-graph node_id to a Sorted node Id
    pub fn get(&self, node_id: usize) -> Option<SortId> {
        let word_size = core::mem::size_of::<u64>();
        let offset = node_id * word_size;
        let bytes = self.data.get(offset..offset + word_size)?;
        let value = u64::from_be_bytes(bytes.try_into().unwrap());
        Some(SortId(value as _))
    }
}

pub fn main() -> Result<()> {
    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    info!("loading MPH...");
    let mph = webgraph::utils::mph::GOVMPH::load("/home/zack/graph/latest/compressed/graph.cmph")?;

    info!("loading node2swhid...");
    let node2swhid = Node2SWHID::load("/home/zack/graph/latest/compressed/graph.node2swhid.bin")?;

    info!("loading order...");
    let order = Order::load("/home/zack/graph/latest/compressed/graph.order")?;

    info!("loading compressed graph into memory (with mmap)...");
    let graph = webgraph::bvgraph::load("/home/zack/graph/latest/compressed/graph")?;

    info!("opening graph.nodes.csv...");
    let file = std::io::BufReader::with_capacity(
        1 << 20,
        std::fs::File::open("/home/zack/graph/latest/compressed/graph.nodes.csv")?,
    );

    // Setup the progress logger for
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Visiting graph...");

    for (sorted_id, line) in file.lines().enumerate() {
        let line = line?;

        let mph_id = mph.get_byte_array(line.as_bytes());
        let node_id = order.get(mph_id as usize).unwrap();

        assert_eq!(
            sorted_id, node_id.0,
            "line {:?} has id {} but mph got {}",
            line, sorted_id, node_id.0
        );
        assert_eq!(line, node2swhid.get(node_id).unwrap().to_string());
    }

    pl.done();

    Ok(())
}
