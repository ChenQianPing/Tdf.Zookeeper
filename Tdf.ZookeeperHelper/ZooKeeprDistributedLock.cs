using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZooKeeperNet;
using static ZooKeeperNet.ZooKeeper;

namespace Tdf.ZookeeperHelper
{
    public class ZooKeeprDistributedLock : IWatcher
    {
        /// <summary>
        /// zk链接字符串
        /// </summary>
        private readonly string _connectString = "127.0.0.1:2181";

        private readonly ZooKeeper _zk;
        private readonly string _root = "/locks";  // 根        
        private readonly string _lockName;         // 竞争资源的标志        
        private string _waitNode;                  // 等待前一个锁        
        private string _myZnode;                   // 当前锁               
        private AutoResetEvent _autoevent;
        private readonly TimeSpan _sessionTimeout = TimeSpan.FromMilliseconds(50000);
        private IList<Exception> _exception = new List<Exception>();

        #region Ctor
        /// <summary>
        /// 创建分布式锁
        /// </summary>
        /// <param name="lockName">竞争资源标志,lockName中不能包含单词lock</param>
        public ZooKeeprDistributedLock(string lockName)
        {
            this._lockName = lockName;
            // 创建一个与服务器的连接            
            try
            {
                _zk = new ZooKeeper(_connectString, _sessionTimeout, this);
                var sw = new Stopwatch();
                sw.Start();

                while (true)
                {
                    if (Equals(_zk.State, States.CONNECTING))
                    {
                        break;
                    }
                    if (Equals(_zk.State, States.CONNECTED))
                    {
                        break;
                    }
                }

                sw.Stop();
                var ts2 = sw.Elapsed;
                Console.WriteLine($@"zoo连接总共花费{ts2.TotalMilliseconds}ms.");

                var stat = _zk.Exists(_root, false);
                if (stat == null)
                {
                    // 创建根节点                    
                    _zk.Create(_root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                throw e;
            }
        }
        #endregion

        #region zookeeper节点的监视器
        /// <summary>        
        /// zookeeper节点的监视器        
        /// </summary>        
        public virtual void Process(WatchedEvent @event)

        {
            if (this._autoevent != null)
            {
                //将事件状态设置为终止状态，允许一个或多个等待线程继续；如果该操作成功，则返回true；否则，返回false
                this._autoevent.Set();
            }
        }
        #endregion

        #region TryLock
        public virtual bool TryLock()
        {
            try
            {
                var splitStr = "_lock_";
                if (_lockName.Contains(splitStr))
                {
                    //throw new LockException("lockName can not contains \\u000B");                
                }
                // 创建临时子节点                
                _myZnode = _zk.Create(_root + "/" + _lockName + splitStr, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EphemeralSequential);
                Console.WriteLine(_myZnode + "    创建完成！ ");

                // 取出所有子节点                
                IList<string> subNodes = _zk.GetChildren(_root, false).ToList<string>();
                // 取出所有lockName的锁                
                IList<string> lockObjNodes = new List<string>();
                foreach (var node in subNodes)
                {
                    if (node.StartsWith(_lockName))
                    {
                        lockObjNodes.Add(node);
                    }
                }
                Array alockObjNodes = lockObjNodes.ToArray();
                Array.Sort(alockObjNodes);
                Console.WriteLine(_myZnode + "==" + lockObjNodes[0]);
                if (_myZnode.Equals(_root + "/" + lockObjNodes[0]))
                {
                    // 如果是最小的节点,则表示取得锁   
                    Console.WriteLine(_myZnode + "    获取锁成功！ ");
                    return true;
                }
                // 如果不是最小的节点，找到比自己小1的节点               
                var subMyZnode = _myZnode.Substring(_myZnode.LastIndexOf("/", StringComparison.Ordinal) + 1);
                _waitNode = lockObjNodes[Array.BinarySearch(alockObjNodes, subMyZnode) - 1];
            }
            catch (KeeperException e)
            {
                throw e;
            }
            return false;
        }


        public virtual bool TryLock(TimeSpan time)
        {
            try
            {
                if (this.TryLock())
                {
                    return true;
                }
                return WaitForLock(_waitNode, time);
            }
            catch (KeeperException e)
            {
                throw e;
            }
        }
        #endregion

        #region WaitForLock
        /// <summary>
        /// 等待锁
        /// </summary>
        /// <param name="lower">需等待的锁节点</param>
        /// <param name="waitTime">等待时间</param>
        /// <returns></returns>
        private bool WaitForLock(string lower, TimeSpan waitTime)
        {
            var stat = _zk.Exists(_root + "/" + lower, true);
            // 判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听            
            if (stat != null)
            {
                Console.WriteLine("Thread " + System.Threading.Thread.CurrentThread.Name + " waiting for " + _root + "/" +
                                  lower);
                _autoevent = new AutoResetEvent(false);
                // 阻止当前线程，直到当前实例收到信号，使用 TimeSpan 度量时间间隔并指定是否在等待之前退出同步域
                bool r = _autoevent.WaitOne(waitTime);
                _autoevent.Dispose();
                _autoevent = null;
                return r;
            }
            else return true;
        }
        #endregion

        #region Unlock
        /// <summary>
        /// 解除锁
        /// </summary>
        public virtual void Unlock()
        {
            try
            {
                Console.WriteLine("unlock " + _myZnode);
                _zk.Delete(_myZnode, -1);
                _myZnode = null;
                _zk.Dispose();
            }
            catch (KeeperException e)
            {
                throw e;
            }
        }
        #endregion

    }
}


/*
 * ZooKeeper 里实现分布式锁的基本逻辑：
 * 
 * 1.zookeeper中创建一个根节点（Locks），用于后续各个客户端的锁操作。
 * 
 * 2.想要获取锁的client都在Locks中创建一个自增序的子节点，
 * 每个client得到一个序号，如果自己的序号是最小的则获得锁。
 * 
 * 3.如果没有得到锁，就监控排在自己前面的序号节点，并且设置默认时间，等待它的释放。
 * 
 * 4.业务操作后释放锁,然后监控自己的节点的client就被唤醒得到锁。
 *  (例如client A需要释放锁，只需要把对应的节点1删除掉，
 *  因为client B已经关注了节点1，那么当节点1被删除后，
 *  zookeeper就会通知client B：你是序号最小的了，可以获取锁了)
 *  释放锁的过程相对比较简单，就是删除自己创建的那个子节点即可。
 *  
 *   Demo01 Demo02为测试场景.
 * 
 */
