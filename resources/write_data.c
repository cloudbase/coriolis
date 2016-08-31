// Copyright 2016 Cloudbase Solutions Srl
// All Rights Reserved.

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define MIN_MSG_SIZE (sizeof(uint64_t) + 1)
#define MAX_MSG_SIZE (100 * 1024 * 1024)

#define ERR_MORE_MSG            -1
#define ERR_DONE                0
#define ERR_READ_MSG_SIZE       1
#define ERR_MSG_SIZE            2
#define ERR_OPEN_FILE           3
#define ERR_DATA                4
#define ERR_IO_OPEN             5
#define ERR_IO_SEEK             6
#define ERR_IO_WRITE            7
#define ERR_IO_CLOSE            8
#define ERR_NO_MEM              9
#define ERR_INVALID_ARGS        10
#define ERR_READ_MSG_ID         11
#define ERR_MSG_SIZE_INFLATED   12
#define ERR_ZLIB                13

int write_msg_id(uint32_t msg_id)
{
    size_t c = fwrite(&msg_id, 1, sizeof(msg_id), stdout);
    if (c != sizeof(msg_id))
       return ERR_IO_WRITE;
    if(fflush(stdout))
        return ERR_IO_WRITE;
    return ERR_DONE;
}

int inflate_buf(uint32_t msg_size, void* buf, uint32_t msg_size_inflated,
                void* inflated_buf)
{
    z_stream strm;
    memset(&strm, 0, sizeof(z_stream));
    int ret = inflateInit(&strm);
    if (ret != Z_OK)
        return ERR_ZLIB;

    strm.avail_in = msg_size;
    strm.next_in = buf;
    strm.avail_out = msg_size_inflated;
    strm.next_out = inflated_buf;

    ret = inflate(&strm, Z_FINISH);
    if(ret != Z_STREAM_END)
        return ERR_ZLIB;

    inflateEnd(&strm);
    return ERR_DONE;
}

int handle_msg(FILE* input_stream)
{
    uint32_t msg_id = 0;
    size_t c = fread(&msg_id, 1, sizeof(uint32_t), input_stream);
    if (c != sizeof(uint32_t))
        return ERR_READ_MSG_ID;

    uint32_t msg_size = 0;
    c = fread(&msg_size, 1, sizeof(uint32_t), input_stream);
    if (c != sizeof(uint32_t))
        return ERR_READ_MSG_SIZE;
    if (!msg_size)
    {
        int err = write_msg_id(msg_id);
        if(err)
            return err;
        return ERR_DONE;
    }
    if (msg_size < MIN_MSG_SIZE || msg_size > MAX_MSG_SIZE)
        return ERR_MSG_SIZE;

    uint32_t msg_size_inflated = 0;
    c = fread(&msg_size_inflated, 1, sizeof(uint32_t), input_stream);
    if (c != sizeof(uint32_t))
        return ERR_MSG_SIZE_INFLATED;
    if (msg_size_inflated != 0 && (msg_size_inflated < MIN_MSG_SIZE ||
            msg_size_inflated > MAX_MSG_SIZE))
        return ERR_MSG_SIZE_INFLATED;

    unsigned char* buf = (unsigned char*)malloc(msg_size);
    if (!buf)
        return ERR_NO_MEM;

    c = fread(buf, 1, msg_size, input_stream);
    if (c != msg_size)
        return ERR_IO_OPEN;

    if(msg_size_inflated)
    {
        unsigned char* inflated_buf = (unsigned char*)malloc(msg_size_inflated);
        if (!inflated_buf)
            return ERR_NO_MEM;

        int err = inflate_buf(msg_size, buf, msg_size_inflated, inflated_buf);
        if(err != ERR_DONE)
        {
            free(inflated_buf);
            return err;
        }

        free(buf);
        buf = inflated_buf;
        msg_size = msg_size_inflated;
    }

    char* path = (char*)buf;
    // strlen is unsafe
    unsigned char* data = (unsigned char*)memchr(path, '\0', msg_size);
    if (!data)
        return ERR_DATA;
    data++;

    uint64_t offset = *((uint64_t*)data);
    data += sizeof(uint64_t);

    // Create an empty file in case it does not exist
    FILE* f = fopen(path, "ab+");
    if (!f)
        return ERR_OPEN_FILE;
    if (fclose(f))
        return ERR_IO_CLOSE;

    // Use rb+ to allow fseek when writing
    f = fopen(path, "rb+");
    if (!f)
        return ERR_OPEN_FILE;
    if (fseek(f, (long)offset, SEEK_SET))
        return ERR_IO_SEEK;

    size_t data_size = msg_size - (data - buf);
    c = fwrite(data, 1, data_size, f);
    if (c != data_size)
        return ERR_IO_WRITE;
    if (fclose(f))
        return ERR_IO_CLOSE;

    // TODO: free also in case of errors
    free(buf);

    int err = write_msg_id(msg_id);
    if(err)
        return err;

    return ERR_MORE_MSG;
}

int main(int argc, char **argv)
{
    FILE* input_stream = NULL;
    if (argc == 2)
    {
        char* input_path = argv[1];
        if (!(input_stream = fopen(input_path, "rb")))
            return ERR_IO_OPEN;
    }
    else if (argc == 1)
        input_stream = stdin;
    else
        return ERR_INVALID_ARGS;

    int err;
    while ((err = handle_msg(input_stream)) == ERR_MORE_MSG);

    if (input_stream != stdin)
        fclose(input_stream);

    return err;
}
