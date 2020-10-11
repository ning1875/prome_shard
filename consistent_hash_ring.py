import mmh3


class ConsistentHashRing(object):
    def __init__(self, replicas=3, nodes=None, ):
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []

        if nodes:
            for node in nodes:
                self.add_node(node)
        self.nodes = nodes

    def add_node(self, node):
        """
        Adds a `node` to the hash ring (including a number of replicas)
        """
        for i in range(self.replicas):
            virtual_node = f"{node}#{i}"
            key = self.gen_key(virtual_node)
            self.ring[key] = node
            self._sorted_keys.append(key)
            # print(f"{virtual_node} --> {key} --> {node}")

        self._sorted_keys.sort()
        # print([self.ring[key] for key in self._sorted_keys])

    def remove_node(self, node):
        """
        Removes `node` from the hash ring and its replicas
        """
        for i in range(self.replicas):
            key = self.gen_key(f"{node}#{i}")
            del self.ring[key]
            self._sorted_keys.remove(key)

    def get_node(self, string_key):
        """
        Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        return self.get_node_pos(string_key)[0]

    def get_node_pos(self, string_key):
        """
        Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None, None

        key = self.gen_key(string_key)
        nodes = self._sorted_keys
        for i in range(len(nodes)):
            node = nodes[i]
            if key < node:
                return self.ring[node], i

        # 如果key > node，那么让这些key落在第一个node上就形成了闭环
        return self.ring[nodes[0]], 0

    def gen_key(self, string_key):
        """
        Given a string key it returns a long value, this long value represents
        a place on the hash ring
        """
        return mmh3.hash(string_key, 32, signed=False)

