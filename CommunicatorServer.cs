using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Networking.Communication
{
    public class CommunicatorServer : ICommunicator
    {
        private TcpListener listener;
        private Dictionary<string, TcpClient> clients = new();
        private Dictionary<string, INotificationHandler> handlers = new();

        public string Start(string serverIP = null, string serverPort = null)
        {
            IPAddress ip = IPAddress.Parse(FindIpAddress());
            int port = FindFreePort(ip);
            listener = new TcpListener(ip, port);
            listener.Start();
            Console.WriteLine("Server started...");

            // Start listening for clients
            ThreadPool.QueueUserWorkItem(AcceptClients);

            return $"{serverIP}:{serverPort}";
        }

        private static string FindIpAddress()
        {
            Trace.WriteLine("[Networking] " +
                "CommunicatorServer.FindIpAddress() function called.");
            try
            {
                // get the IP address of the machine
                IPHostEntry host = Dns.GetHostEntry(Dns.GetHostName());

                // iterate through the ip addresses and return the
                // address if it is IPv4 and does not end with 1
                foreach (IPAddress ipAddress in host.AddressList)
                {
                    // check if the address is IPv4 address
                    if (ipAddress.AddressFamily == 
                        AddressFamily.InterNetwork)
                    {
                        string address = ipAddress.ToString();
                        // return the IP address if it does not end
                        // with 1, as the loopback address ends with 1
                        if (address.Split(".")[3] != "1")
                        {
                            return ipAddress.ToString();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Trace.WriteLine("[Networking] Error in " +
                    "CommunicatorServer.FindIpAddress(): " +
                    e.Message);
                return "null";
            }
            throw new Exception("[Networking] Error in " +
                "CommunicatorServer.FindIpAddress(): IPv4 address " +
                "not found on this machine!");
        }

        /// <summary>
        /// Finds a free TCP port on the current machine for the given
        /// IP address.
        /// </summary>
        /// <param name="ipAddress">
        /// IP address for which to find the free port.
        /// </param>
        /// <returns> The port number </returns>
        private static int FindFreePort(IPAddress ipAddress)
        {
            Trace.WriteLine("[Networking] " +
                "CommunicatorServer.FindFreePort() function called.");
            try
            {
                // start a tcp listener on port = 0, the tcp listener
                // will be assigned a port number
                TcpListener tcpListener = new(ipAddress, 0);
                tcpListener.Start();

                // return the port number of the tcp listener
                int port = 
                    ((IPEndPoint)tcpListener.LocalEndpoint).Port;
                tcpListener.Stop();
                return port;
            }
            catch (Exception e)
            {
                Trace.WriteLine("[Networking] Error in " +
                    "CommunicatorServer.FindFreePort(): " +
                    e.Message);
                return -1;
            }
        }

 
        private void AcceptClients(object state)
        {
            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                string clientId = client.Client.RemoteEndPoint.ToString();
                clients[clientId] = client;
                Console.WriteLine($"Client {clientId} connected.");

                // Notify handlers
                foreach (var handler in handlers.Values)
                {
                    handler.OnClientJoined(client);
                }

                // Start listening for data from the client
                ThreadPool.QueueUserWorkItem(ReceiveData, client);
            }
        }

        private void ReceiveData(object clientObj)
        {
            TcpClient client = (TcpClient)clientObj;
            string clientId = client.Client.RemoteEndPoint.ToString();

            while (true)
            {
                NetworkStream stream = client.GetStream();
                byte[] buffer = new byte[1024];
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0)
                {
                    Console.WriteLine($"Client {clientId} disconnected.");
                    clients.Remove(clientId);

                    foreach (var handler in handlers.Values)
                    {
                        handler.OnClientLeft(clientId);
                    }
                    break;
                }

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

            if (destination == null)
            {
                // Broadcast to all clients
                foreach (var client in clients.Values)
                {
                    client.GetStream().Write(buffer, 0, buffer.Length);
                }
            }
            else if (clients.TryGetValue(destination, out TcpClient client))
            {
                // Send to a specific client
                client.GetStream().Write(buffer, 0, buffer.Length);
            }
        }

        public void Subscribe(string moduleName, INotificationHandler notificationHandler, bool isHighPriority = false)
        {
            handlers[moduleName] = notificationHandler;
        }

        public void Stop()
        {
            listener.Stop();
            foreach (var client in clients.Values)
            {
                client.Close();
            }
        }

        public void AddClient(string clientId, TcpClient socket)
        {
            clients[clientId] = socket;
        }

        public void RemoveClient(string clientId)
        {
            clients.Remove(clientId);
        }

        public Dictionary<string, TcpClient> GetClientList()
        {
            return clients;
        }
    }
}
