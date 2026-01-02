use anyhow::Result;
use clap::{Parser, Subcommand};
use memberlist::{Memberlist, Options};
use memberlist::net::{NetTransport, NetTransportOptions};
use memberlist::net::resolver::socket_addr::SocketAddrResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::tokio::TokioRuntime;
use memberlist::proto::NodeState;
use memberlist::bytes::Bytes;
use memberlist::proto::Meta;
use smol_str::SmolStr;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::signal;
use tokio::time::{Duration, interval};
use tracing::{info, Level};
use tracing_subscriber;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::RwLock;
use std::collections::HashSet;

// ==================== Message Protocol Definition ====================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ClusterState {
    // 主节点名称（用于领导权识别）
    pub master_node: Option<String>,
    // 集群配置版本（用于冲突解决）
    pub config_version: u64,
    // 最后更新时间戳
    pub last_updated: u64,
    // 自定义状态字段（示例）
    pub active_jobs: u32,
    pub certificate_expiry_warning: bool,
    pub states_context: String
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ClusterMessage {
    Heartbeat {
        from_node: String,
        timestamp: u64,
        sequence: u32,
    },
    MasterStatus {
        node_name: String,
        timestamp: u64,
        member_count: u32,
    },
    Command {
        command_type: String,
        payload: String,
    },
    StateUpdate {
        new_state: ClusterState,
        source_node: String,
    },
}

/// Simplified delegate implementation - adapted for memberlist 0.7.0 API
#[derive(Clone)]
pub struct SimpleDelegate {
    received_count: Arc<AtomicUsize>,
    broadcast_queue: Arc<Mutex<VecDeque<Bytes>>>,
    is_master: bool,
    cluster_state: Arc<RwLock<ClusterState>>,
    seen_state_versions: Arc<Mutex<HashSet<(String, u64)>>>,
    
    current_meta: Arc<Mutex<Meta>>,
    node_name: String,
}

impl SimpleDelegate {
    pub fn new(is_master: bool, node_name: String) -> Self {
        Self {
            received_count: Arc::new(AtomicUsize::new(0)),
            broadcast_queue: Arc::new(Mutex::new(VecDeque::new())),
            is_master,
            cluster_state: Arc::new(RwLock::new(ClusterState::default())),
            seen_state_versions: Arc::new(Mutex::new(HashSet::new())),
            current_meta: Arc::new(Mutex::new(Meta::empty())),
            node_name,
        }
    }
    pub fn update_meta(&self, new_meta: Vec<u8>) {
        // 使用 TryFrom 将 Vec<u8> 转换为 Meta
        match Meta::try_from(new_meta) {
            Ok(meta) => {
                let mut current = self.current_meta.lock().unwrap();
                *current = meta;
            }
            Err(e) => {
                tracing::error!("Failed to convert meta: {:?}", e);
            }
        }
    }
    
    pub fn get_meta(&self) -> Meta {
        let meta = self.current_meta.lock().unwrap();
        meta.clone()
    }

    pub fn received_count(&self) -> usize {
        self.received_count.load(Ordering::SeqCst)
    }

    pub fn add_broadcast_message(&self, message: ClusterMessage) {
        if !self.is_master {
            // 只有主节点可以添加广播消息
            return;
        }
        
        match bincode::serialize(&message) {
            Ok(bytes) => {
                let mut queue = self.broadcast_queue.lock().unwrap();
                queue.push_back(Bytes::from(bytes));
                tracing::debug!("Added broadcast message to queue: {:?}", message);
            }
            Err(e) => {
                tracing::error!("Failed to serialize broadcast message: {}", e);
            }
        }
    }

    pub fn get_cluster_state(&self) -> ClusterState {
        self.cluster_state.read().unwrap().clone()
    }
    
    pub fn update_cluster_state(&self, new_state: ClusterState, source_node: &str) -> bool {
        // 检查是否已经处理过此版本
        let version_key = (source_node.to_string(), new_state.config_version);
        {
            let mut seen = self.seen_state_versions.lock().unwrap();
            if seen.contains(&version_key) {
                tracing::debug!("Ignoring duplicate state version from {}", source_node);
                return false;
            }
            seen.insert(version_key);
            // 限制集合大小
            if seen.len() > 100 {
                let _ = seen.drain().take(50).collect::<Vec<_>>();
            }
        }
        
        // 简单的冲突解决：接受版本号更高的状态
        let mut current_state = self.cluster_state.write().unwrap();
        if new_state.config_version > current_state.config_version {
            *current_state = new_state;
            tracing::info!("State updated to version {}", current_state.config_version);
            true
        } else {
            tracing::debug!("Ignoring stale state version (current: {}, incoming: {})", 
                current_state.config_version, new_state.config_version);
            false
        }
    }
    
    // 主节点修改状态的方法
    pub fn modify_state<F>(&self, mutator: F) -> Option<ClusterState>
    where
        F: FnOnce(&mut ClusterState),
    {
        if !self.is_master {
            tracing::warn!("Only master node can modify state");
            return None;
        }
        
        let mut current_state = self.cluster_state.write().unwrap();
        mutator(&mut current_state);
        current_state.config_version += 1;
        current_state.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        current_state.master_node = Some(self.node_name.clone());
        
        Some(current_state.clone())
    }

    fn handle_message(&self, data: &[u8]) {
        match bincode::deserialize::<ClusterMessage>(data) {
            Ok(message) => {
                self.received_count.fetch_add(1, Ordering::SeqCst);
                self.add_broadcast_message(message.clone());
                match message {
                    ClusterMessage::Heartbeat { from_node, timestamp, sequence } => {
                        tracing::debug!("Received heartbeat: {} seq={} ts={}", from_node, sequence, timestamp);
                    }
                    ClusterMessage::MasterStatus { node_name, member_count, .. } => {
                        tracing::info!("Received master node status: {} member count={}", node_name, member_count);
                    }
                    ClusterMessage::Command { command_type, payload } => {
                        tracing::info!("Received command: {} = {}", command_type, payload);
                    }
                    ClusterMessage::StateUpdate { new_state, source_node } => {
                        let updated = self.update_cluster_state(new_state, &source_node);
                        if updated {
                            let state = self.get_cluster_state();
                            tracing::info!(
                                "State synchronized: version={}, master={:?}, jobs={}",
                                state.config_version,
                                state.master_node,
                                state.active_jobs
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to parse message: {}", e);
            }
        }
    }
}

impl memberlist::delegate::NodeDelegate for SimpleDelegate {
    fn notify_message(
        &self,
        msg: std::borrow::Cow<'_, [u8]>,
    ) -> impl std::future::Future<Output = ()> + Send {
        let data = msg.into_owned();
        let delegate = self.clone();
        async move {
            tokio::spawn(async move {
                delegate.handle_message(&data);
            });
        }
    }
    //在meta版本代码中，该函数没有作用
    fn broadcast_messages<F>(
        &self,
        limit: usize,
        _encoded_len: F,
    ) -> impl std::future::Future<Output = impl Iterator<Item = memberlist::bytes::Bytes> + Send> + Send
    where
        F: Fn(memberlist::bytes::Bytes) -> (usize, memberlist::bytes::Bytes) + Send + Sync + 'static,
    {
    
        let queue = self.broadcast_queue.clone();
        async move {
            let mut messages = Vec::new();
            let mut queue = queue.lock().unwrap();
            for _ in 0..limit {
                if let Some(msg) = queue.pop_front() {
                    messages.push(msg);
                } else {
                    break;
                }
            }
            messages.into_iter()
        }
    }

    fn node_meta(
        &self,
        _limit: usize,
    ) -> impl std::future::Future<Output = Meta> + Send {
        let meta = self.get_meta();
        async { meta }
    }
}

impl memberlist::delegate::AliveDelegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;
    type Error = std::io::Error;

    /// Called when a node joins, used to verify if the node is alive
    async fn notify_alive(
        &self,
        _node: Arc<NodeState<SmolStr, std::net::SocketAddr>>
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl memberlist::delegate::MergeDelegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;
    type Error = std::io::Error;

    async fn notify_merge(
        &self,
        _peers: Arc<[NodeState<Self::Id, Self::Address>]>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl memberlist::delegate::ConflictDelegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn notify_conflict(
        &self,
        _left: Arc<NodeState<Self::Id, Self::Address>>,
        _right: Arc<NodeState<Self::Id, Self::Address>>,
    ) {
        // Use default handling for conflicts, no additional actions needed
    }
}

/// Implement EventDelegate - for event notifications
impl memberlist::delegate::EventDelegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        // 当节点的元数据更新时，这个回调会被触发

        let meta = node.meta();
    
        // 检查是否有元数据（即使设置了空元数据，meta() 也会返回一个空的 Bytes）
        if meta.is_empty() {
            // 节点可能没有设置元数据，或者设置了空的元数据
            tracing::trace!("Node {} has no meta data or empty meta", node.id());
            return;
        }

        match bincode::deserialize::<ClusterState>(meta) {
            Ok(new_state) => {
                let source_node = node.id().to_string();
                self.update_cluster_state(new_state, &source_node);
                
                tracing::debug!(
                    "Node {} updated its state (version: {})",
                    node.id(),
                    self.get_cluster_state().config_version
                );
            }
            Err(e) => {
                tracing::warn!("Failed to parse meta from {}: {}", node.id(), e);
            }
        }
    }
    
    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        tracing::info!("Node {} joined the cluster", node.id());
        // 可以在这里查询新节点的状态
    }
    
    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        tracing::info!("Node {} left the cluster", node.id());
    }
}

/// Implement PingDelegate - for handling ping requests
impl memberlist::delegate::PingDelegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn ack_payload(&self) -> memberlist::bytes::Bytes {
        memberlist::bytes::Bytes::new()
    }

    async fn notify_ping_complete(
        &self,
        _node: Arc<NodeState<Self::Id, Self::Address>>,
        _duration: std::time::Duration,
        _bytes: memberlist::bytes::Bytes,
    ) {
    }
}

impl memberlist::delegate::Delegate for SimpleDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;
}

// ==================== Command Line Argument Definition ====================

#[derive(Parser)]
#[command(name = "memberlist-node", version = "1.0", about = "Memberlist cluster node")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Start {
        #[arg(short, long, help = "Node name")]
        name: String,
        
        #[arg(short, long, help = "Binding address", default_value = "0.0.0.0:7946")]
        bind: SocketAddr,
        
        #[arg(short, long, help = "Seed node address to join")]
        join: Option<SocketAddr>,
        
        #[arg(long, help = "Set as master node", default_value = "false")]
        master: bool,
        
        #[arg(long, help = "Broadcast interval (seconds)", default_value = "5")]
        interval: u64,
    },
}

// ==================== Main Function ====================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { name, bind, join, master, interval } => {
            run_node(name, bind, join, master, interval).await?;
        }
    }

    Ok(())
}

// ==================== Core Logic ====================

async fn run_node(
    name: String,
    bind: SocketAddr,
    join: Option<SocketAddr>,
    is_master: bool,
    broadcast_interval: u64,
) -> Result<()> {
    info!("Starting node: {} @ {}", name, bind);
    if let Some(ref j) = join {
        info!("Will join: {}", j);
    } else {
        info!("Starting as seed node");
    }

    // Create delegate
    let delegate = SimpleDelegate::new(is_master, name.clone());

    // Create Options configuration
    let options = Options::lan()
                .with_gossip_nodes(1)
                .with_gossip_interval(Duration::from_secs(1))
                .with_probe_interval(Duration::from_secs(1));

    tracing::info!("Gossip nodes: {:?}", options.gossip_nodes());
    tracing::info!("Gossip interval: {:?}", options.gossip_interval());
    tracing::info!("Gossip to the dead time: {:?}", options.gossip_to_the_dead_time());
    // Create transport layer options
    let mut bind_addresses = indexmap::IndexSet::new();
    bind_addresses.insert(bind);

    let transport_options: NetTransportOptions<SmolStr, SocketAddrResolver<TokioRuntime>, Tcp<TokioRuntime>>  = NetTransportOptions::<_, SocketAddrResolver<TokioRuntime>, _>::new(SmolStr::from(name.clone()))
    .with_bind_addresses(bind_addresses);

    let memberlist:Memberlist<NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tcp<TokioRuntime>, TokioRuntime>, SimpleDelegate>  = Memberlist::with_delegate(
        delegate.clone(), 
        transport_options,  
        options
    ).await?;

    info!("Memberlist node created successfully");

    // Join cluster
    if let Some(join_addr) = join {
        let unresolved_addr = memberlist::proto::MaybeResolvedAddress::unresolved(join_addr);
        let node = memberlist::net::Node::new(SmolStr::from("seed-node"), unresolved_addr);
        match memberlist.join(node).await {
            Ok(n) => info!("Successfully joined {} nodes", n),
            Err(e) => {
                info!("Join failed: {}, running as seed node", e);
            }
        }
    }

    // Master node broadcast task
    if is_master {
        tokio::spawn(state_update_task(delegate.clone(), memberlist.clone(), broadcast_interval));
    }

    info!("Node is online, press Ctrl+C to exit");

    // 创建初始的状态
    let initial_state = ClusterState {
        master_node: if is_master { Some(name.clone()) } else { None },
        config_version: 1,
        last_updated: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        active_jobs: 0,
        certificate_expiry_warning: false,
        states_context: "".to_string(),
    };
    
    // 将状态序列化为元数据
    let meta_bytes = bincode::serialize(&initial_state)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    
    // 设置节点元数据（memberlist会自动同步这些数据）
    delegate.update_meta(meta_bytes);
    
    if let Err(e) = memberlist.update_node(Duration::from_secs(5)).await {
        tracing::error!("Failed to update initial node meta: {}", e);
    }
    // 初始化delegate的状态
    delegate.update_cluster_state(initial_state, "init");
    
    info!("Node started with initial state (version: 1)");

    // Periodic reporting
    let mut report_interval = interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = report_interval.tick() => {
                let count = memberlist.num_members().await;

                let state = delegate.get_cluster_state();
                info!(
                    "Cluster: members={}, received_msgs={}, state_version={}, master={:?}, active_jobs={}, cert_warning={}",
                    count,
                    delegate.received_count(),
                    state.config_version,
                    state.master_node,
                    state.active_jobs,
                    state.certificate_expiry_warning
                );
            }
            _ = signal::ctrl_c() => {
                info!("Shutting down node...");
                break;
            }
        }
    }

    

    memberlist.shutdown().await?;
    info!("Node has been shut down");
    Ok(())
}

/// State update task - 主节点定期修改并同步状态
async fn state_update_task<D: memberlist::delegate::Delegate<Id = SmolStr, Address = SocketAddr> + Clone + 'static>(
    delegate: SimpleDelegate,
    memberlist: Memberlist<NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tcp<TokioRuntime>, TokioRuntime>, D>,
    // node_name: String, 
    update_interval_secs: u64
) {
    let mut interval = interval(Duration::from_secs(update_interval_secs));
    let mut job_counter = 0u32;

    loop {
        interval.tick().await;
        
        // 只有主节点才修改状态
        if let Some(updated_state) = delegate.modify_state(|state| {
            // 模拟状态变化：增加活跃作业数
            state.active_jobs = job_counter % 10; // 循环0-9
            job_counter = job_counter.wrapping_add(1);
            
            // 模拟证书过期警告
            state.certificate_expiry_warning = (job_counter % 20) == 0;
        }) {
            tracing::info!(
                "Master updated state to version {} (jobs: {}, expiry warning: {})",
                updated_state.config_version,
                updated_state.active_jobs,
                updated_state.certificate_expiry_warning
            );
            
            // 将新状态设置为节点的元数据（这会触发自动同步）
            // 更新 delegate 中的元数据，然后调用 update_node
            match bincode::serialize(&updated_state) {
                Ok(meta_bytes) => {
                    delegate.update_meta(meta_bytes);
                    if let Err(e) = memberlist.update_node(Duration::from_secs(5)).await {
                        tracing::error!("Failed to update node meta: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to serialize state: {}", e);
                }
            }
        }
    }
}

