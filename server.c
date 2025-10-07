#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <semaphore.h>

#define TABLE_SIZE 1024
#define MAX_BUFFER_SIZE 256

sem_t mutex;

void error(char *msg)
{
    perror(msg);
    exit(1);
}

typedef struct KeyValue
{
    int key;
    char *value;
    struct KeyValue *next;
} KeyValue;

KeyValue *table[TABLE_SIZE] = {NULL};

int hash(int key)
{
    return key % TABLE_SIZE;
}

KeyValue *createNode(int key, char *value)
{
    KeyValue *newNode = (KeyValue *)malloc(sizeof(KeyValue));

    if (newNode == NULL)
    {
        return NULL;
    }

    newNode->key = key;
    // strdup allocates memory and copies the string
    newNode->value = strdup(value);

    // if strdup fails
    if (newNode->value == NULL)
    {
        free(newNode);
        return NULL;
    }

    newNode->next = NULL;

    return newNode;
}

void insert(KeyValue **table, int key, char *value)
{
    int index = hash(key);

    KeyValue *newNode = createNode(key, value);

    newNode->next = table[index];
    table[index] = newNode;
}

char *search(KeyValue **table, int key)
{
    int index = hash(key);

    KeyValue *current = table[index];

    while (current != NULL)
    {
        if (key == current->key)
        {
            return current->value;
        }
        current = current->next;
    }

    return NULL;
}

int update(KeyValue **table, int key, char *newValue)
{
    int index = hash(key);

    KeyValue *current = table[index];

    while (current != NULL)
    {
        if (key == current->key)
        {
            free(current->value);
            current->value = strdup(newValue);
            return 1;
        }

        current = current->next;
    }
    // update failed, key not found
    return 0;
}

int delete(KeyValue **table, int key)
{
    int index = hash(key);

    KeyValue *current = table[index];
    KeyValue *prev = NULL;

    while (current != NULL)
    {
        if (key == current->key)
        {
            if (prev == NULL)
            {
                // deleting head node
                table[index] = current->next;
            }
            else
            {
                prev->next = current->next;
            }

            free(current->value);
            free(current);
            return 1;
        }

        prev = current;
        current = current->next;
    }
    // deletion failed, key not found
    return 0;
}

void* handle_client(void *newsockfd){
    int n;
    char buffer[MAX_BUFFER_SIZE];
    int sock = *(int *)newsockfd;

    while (1)
    {
        bzero(buffer, MAX_BUFFER_SIZE);
        int n;
        n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
        if (n < 0)
        {
            error("ERROR reading from socket");
        }
        else if (n == 0)
        {
            printf("> Client disconnected.\n");
            close(sock);
            break;
        }
        buffer[n] = '\0';

        char *command = strtok(buffer, " ");
        printf("Command : %s", command);

        if (strcmp(command, "create") == 0)
        {
            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            int key = atoi(buffer);

            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            int value_size = atoi(buffer);

            int received_bytes = 0;
            char *value = (char *)malloc(value_size + 1);

            while (received_bytes < value_size)
            {
                int len_to_recieve;
                if (value_size - received_bytes > MAX_BUFFER_SIZE - 1)
                {
                    len_to_recieve = MAX_BUFFER_SIZE - 1;
                }
                else
                {
                    len_to_recieve = value_size - received_bytes;
                }

                n = read(sock, value + received_bytes, len_to_recieve);
                if (n < 0)
                {
                    error("ERROR reading from socket");
                }
                received_bytes += n;
            }

            value[value_size] = '\0';

            char *response;
            sem_wait(&mutex);
            if (search(table, key) != NULL)
            {
                response = "Error: Key already exists";
            }
            else
            {
                insert(table, key, value);
                response = "Key-Value pair created successfully";
            }
            sem_post(&mutex);

            n = write(sock, response, strlen(response) + 1);
            if (n < 0)
            {
                error("ERROR writing to socket");
            }

            printf(" (Key: %d, Value: %s)\n", key, value);
            free(value);
        }

        else if(strcmp(command, "read") == 0)
        {
            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            int key = atoi(buffer);

            printf(" (Key: %d)\n", key);
            sem_wait(&mutex);
            char *value = search(table, key);
            sem_post(&mutex);
            if (value == NULL){
                char *response = "ERROR Error: Key not found";
                bzero(buffer, MAX_BUFFER_SIZE);
                strcpy(buffer, response);

                n = write(sock, buffer, MAX_BUFFER_SIZE-1);
                if(n < 0){
                    error("ERROR writing to socket");
                }
            }
            else{
                int value_size = strlen(value);
                bzero(buffer, MAX_BUFFER_SIZE);
                snprintf(buffer, MAX_BUFFER_SIZE, "OK %d", value_size);

                n = write(sock, buffer, MAX_BUFFER_SIZE-1);
                if(n < 0){
                    error("ERROR writing to socket");
                }

                int sent_bytes = 0;

                while(sent_bytes < value_size){
                    int len_to_send;
                    if(value_size - sent_bytes > MAX_BUFFER_SIZE - 1){
                        len_to_send = MAX_BUFFER_SIZE - 1;
                    }
                    else{
                        len_to_send = value_size - sent_bytes;
                    }

                    n = write(sock, value + sent_bytes, len_to_send);
                    if(n < 0){
                        error("ERROR writing to socket");
                    }
                    sent_bytes += n;
                }
            }
        }

        else if(strcmp(command, "update") == 0)
        {
            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            int key = atoi(buffer);

            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            int value_size = atoi(buffer);

            int received_bytes = 0;
            char *newValue = (char *)malloc(value_size + 1);

            while (received_bytes < value_size)
            {
                int len_to_recieve;
                if (value_size - received_bytes > MAX_BUFFER_SIZE - 1)
                {
                    len_to_recieve = MAX_BUFFER_SIZE - 1;
                }
                else
                {
                    len_to_recieve = value_size - received_bytes;
                }

                n = read(sock, newValue + received_bytes, len_to_recieve);
                if (n < 0)
                {
                    error("ERROR reading from socket");
                }
                received_bytes += n;
            }

            newValue[value_size] = '\0';

            char *response;
            sem_wait(&mutex);
            if (update(table, key, newValue))
            {
                response = "Key-Value pair updated successfully";
            }
            else
            {
                response = "Error: Key not found";
            }
            sem_post(&mutex);
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, response);
            n = write(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR writing to socket");
            }

            printf(" (Key: %d, New Value: %s)\n", key, newValue);
            free(newValue);
        }

        else if (strcmp(command, "delete") == 0)
        {
            n = read(sock, buffer, MAX_BUFFER_SIZE - 1);
            if (n < 0)
            {
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            int key = atoi(buffer);

            char *response;
            sem_wait(&mutex);
            if (delete(table, key))
            {
                response = "Key-Value pair deleted successfully";
            }
            else
            {
                response = "Error: Key not found";
            }
            sem_post(&mutex);
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, response);
            n = write(sock, buffer, MAX_BUFFER_SIZE - 1 );
            if (n < 0)
            {
                error("ERROR writing to socket");
            }

            printf(" (Key: %d)\n", key);
        }
    }
}

int main(int argc, char *argv[])
{

    sem_init(&mutex, 0, 1);
    int sockfd, newsockfd, portno, clilen;
    char buffer[MAX_BUFFER_SIZE];

    struct sockaddr_in serv_addr, cli_addr;

    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <IP address> <Port number>\n", argv[0]);
        exit(1);
    }

    // creating a TCP internet socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
    {
        error("ERROR opening socket");
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    portno = atoi(argv[2]);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(portno);

    // convert IPv4 addresses from text to binary form
    if (inet_pton(AF_INET, argv[1], &serv_addr.sin_addr) <= 0)
    {
        struct hostent *server = gethostbyname(argv[1]);

        if (server == NULL)
        {
            fprintf(stderr, "ERROR, no such host\n");
            exit(1);
        }

        // copying the first address in the list to serv_addr.sin_addr.s_addr
        // h_addr_list is a NULL-terminated array of network addresses (in network byte order) for the host
        bcopy((char *)server->h_addr_list[0], (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    }

    // binding the socket to the address and port number specified in serv_addr structure
    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        error("ERROR on binding");
    }

    // listen for incoming connection requests
    listen(sockfd, 5);
    clilen = sizeof(cli_addr);

    // accept a new request, create a newsockfd
    while (1)
    {
        newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0)
        {
            error("ERROR on accept");
        }

        printf("> Client with IP address %s and port %d connected.\n", inet_ntoa(cli_addr.sin_addr), ntohs(cli_addr.sin_port));

        pthread_t thread_id;
        pthread_create(&thread_id, NULL, handle_client, &newsockfd);
        
    }
    return 0;
}