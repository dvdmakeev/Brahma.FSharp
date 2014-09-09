using System.Collections.Generic;

namespace AgentUtils
{
  public class DataPool<TData>
  {
    private readonly List<TData> pool;

    private readonly object locker;

    public DataPool()
    {
      pool = new List<TData>();
      locker = new object();
    }

    public int GetDataCount
    {
      get
      {
        lock (locker)
        {
          return pool.Count;
        }
      }
    }

    public bool HasData
    {
      get
      {
        lock (locker)
        {
          return pool.Count != 0;
        }
      }
    }

    public void AddData(TData data)
    {
      lock (locker)
      {
        pool.Add(data);
      }
    }

    public TData GetData()
    {
      while (true)
      {
        var dataIndex = pool.Count - 1;

        if (dataIndex > 0)
        {
          lock (locker)
          {
            var data = pool[dataIndex];
            pool.RemoveAt(dataIndex);

            return data;
          }
        }
      }
    }
  }
}
