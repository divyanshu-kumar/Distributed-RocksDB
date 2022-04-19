#include <experimental/filesystem>

class ReadCache {
    unordered_map<int, string> inMemoryCachedBlocks;
    bool inMemoryCacheEnable;
    mutex m_lock;
    
    public:

    ReadCache() : inMemoryCacheEnable(false) {}

    bool isCacheEnabled() {
        return inMemoryCacheEnable;
    }

    void insert(const string & buffer, const int & address) {
        m_lock.lock();
        inMemoryCachedBlocks[address] = buffer;
        m_lock.unlock();
    }

    bool isPresent(const int & address) {
        bool result;
        m_lock.lock();
        result = inMemoryCachedBlocks.find(address) != inMemoryCachedBlocks.end();
        m_lock.unlock();
        return result;
    }

    string getCached(const int & address) {
        string result;
        m_lock.lock();
        result = inMemoryCachedBlocks[address];
        m_lock.unlock();
        return result;
    }

    bool isEnabled() {
        return inMemoryCacheEnable;
    }
};

// do a force copy of file from source to destination
int copyFile(const string &to, const string &from){
    std::experimental::filesystem::path sourceFile = from;
    std::experimental::filesystem::path target = to;

    try
    {
        copy_file(sourceFile, target, std::experimental::filesystem::copy_options::overwrite_existing);
    }
    catch (std::exception& e) // Not using fs::filesystem_error since std::bad_alloc can throw too.  
    {
        std::cout << e.what() << "Error occured while copying the file " << from << " to destinatoon " << to << endl;
        return -1;
    }
    return 0;
}

int copyFile(const char *to, const char *from) {
    int fd_to, fd_from;
    char buf[4096];
    ssize_t nread;
    int saved_errno;

    fd_from = open(from, O_RDONLY);
    if (fd_from < 0)
        return -1;

    fd_to = open(to, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_to < 0) {
        goto out_error;
    }

    while (nread = read(fd_from, buf, sizeof buf), nread > 0) {
        char *out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);
            if (nwritten >= 0) {
                nread -= nwritten;
                out_ptr += nwritten;
            }
            else if (errno != EINTR) {
                goto out_error;
            }
        } while (nread > 0);
    }

    if (nread == 0) {
        if (close(fd_to) < 0) {
            fd_to = -1;
            goto out_error;
        }
        close(fd_from);

        /* Success! */
        return 0;
    }

  out_error:
    saved_errno = errno;

    close(fd_from);
    if (fd_to >= 0)
        close(fd_to);

    errno = saved_errno;
    return -1;
}