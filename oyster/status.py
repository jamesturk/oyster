from client import Client

if __name__ == '__main__':
    c = Client()

    print 'Tracking:', c.db.tracked.count()
    print 'Waiting In Queue:', c.get_update_queue_size()
