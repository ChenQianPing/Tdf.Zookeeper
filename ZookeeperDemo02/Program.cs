using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Tdf.ZookeeperHelper;

namespace ZookeeperDemo02
{
    class Program
    {
        static void Main(string[] args)
        {
            int count = 1; // 库存 商品编号1079233
            if (count == 1)
            {
                var zklock = new ZooKeeprDistributedLock("Getorder_Pid1079233");
                // 创建锁
                if (zklock.TryLock(TimeSpan.FromMilliseconds(50000)))
                {
                    Console.WriteLine("Demo2创建订单成功！");
                }
                else
                {
                    Console.WriteLine("Demo2创建订单失败了！");
                }

                Thread.Sleep(30000); // 对操作释放锁进行阻塞
                Console.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss")); // 要进行释放锁的操作时间 主要测试当前锁释放后 Demo1的节点监控是否唤起
                zklock.Unlock();     // 释放锁
                Console.ReadKey();
            }
        }
    }
}
