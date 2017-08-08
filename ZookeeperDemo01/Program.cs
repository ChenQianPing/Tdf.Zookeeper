using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tdf.ZookeeperHelper;

namespace ZookeeperDemo01
{
    class Program
    {
        static void Main(string[] args)
        {
            var count = 1; // 库存 商品编号1079233
            if (count == 1)
            {
                var zklock = new ZooKeeprDistributedLock("Getorder_Pid1079233");
                if (zklock.TryLock())
                {
                    Console.WriteLine("Demo1创建订单成功！");
                }
                else
                {
                    Console.WriteLine("Demo1创建订单失败了！");
                }

                Console.WriteLine(DateTime.Now.ToString("yyyyMMdd HH:mm:ss"));

                zklock.Unlock();

                Console.ReadKey();
            }

        }
    }
}
