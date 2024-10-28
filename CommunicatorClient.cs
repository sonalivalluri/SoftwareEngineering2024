using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Networking.Communication
{
    public class CommunicatorClient : ICommunicator
    {
        private TcpClient client;
        private Dictionary<string, INotificationHandler> handlers = new();

        public string Start(string serverIP = null, string serverPort = null)
        {
            try
            {
                client = new TcpClient(serverIP, int.Parse(serverPort));
                ThreadPool.QueueUserWorkItem(ReceiveData);
                return "success";
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to connect: {e.Message}");
                return "failure";
            }
        }

        private void ReceiveData(object state)
        {
            while (true)
            {
                NetworkStream stream = client.GetStream();
                byte[] buffer = new byte[1024];
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;

                string receivedData = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                string[] packetParts = receivedData.Split(new[] { ':' }, 2);
                if (packetParts.Length == 2)
                {
                    string module = packetParts[0];
                    string data = packetParts[1];

                    if (handlers.TryGetValue(module, out INotificationHandler handler))
                    {
                        handler.OnDataReceived(data);
                    }
                }
            }
        }

        public void Send(string serializedData, string moduleOfPacket, string? destination)
        {
            string packet = $"{moduleOfPacket}:{serializedData}";
            byte[] buffer = Encoding.UTF8.GetBytes(packet);
            client.GetStream().Write(buffer, 0, buffer.Length);
        }

        public void Subscribe(string moduleName, INotificationHandler notificationHandler, bool isHighPriority = false)
        {
            handlers[moduleName] = notificationHandler;
        }

        public void Stop()
        {
            client.Close();
        }

        public void AddClient(string clientId, TcpClient socket) { }

        public void RemoveClient(string clientId) { }

        public Dictionary<string, TcpClient> GetClientList() { return null; }
    }
}
