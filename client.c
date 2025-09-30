#include<stdio.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<stdlib.h>
#include<unistd.h>
#include<string.h>
#include<netdb.h>
#include<arpa/inet.h>

#define MAX_BUFFER_SIZE 256

void error(char* msg){
    perror(msg);
    exit(1);
}

int main(int argc, char* argv[])
{
    int sockfd = -1, n;
    char *line = NULL;
    size_t len = 0;
    char buffer[MAX_BUFFER_SIZE];

    if(argc < 2 || argc > 3){
        fprintf(stderr, "usage: %s interactive|batch <filename>\n", argv[0]);
        exit(1);
    }

    FILE *input_stream = NULL;

    if(strcmp(argv[1], "batch") == 0){
        if(argc != 3){
            fprintf(stderr, "usage: %s batch <filename>\n", argv[0]);
            exit(1);
        }

        input_stream = fopen(argv[2], "r");
        if(input_stream == NULL){
            error("Error opening file");
        }
    }
    else if(strcmp(argv[1], "interactive") == 0){
        if(argc != 2){
            fprintf(stderr, "usage: %s interactive\n", argv[0]);
            exit(1);
        }

        input_stream = stdin;
        printf("Entering interactive mode. Type your commands below:\n");
    }
    else{
        error("Invalid mode. Use 'interactive' or 'batch'.\n");
    }

    while(1){
        if(input_stream == stdin){
            printf("$ ");
        }
        int read_bytes = getline(&line, &len, input_stream);

        if(read_bytes < 0){
            if(line){
                free(line);
            }
            if(input_stream != stdin){
                fclose(input_stream);
            }
            printf("End of input. Exiting client program.\n");
            break;
        }

        //remove trailing newline character
        if(read_bytes > 0 && line[read_bytes - 1] == '\n'){
            line[read_bytes - 1] = '\0';
            read_bytes--;
        }

        //empty command
        if(read_bytes == 0){
            continue;
        }

        char *command = strtok(line, " \n");
        if(command == NULL){
            continue;
        }

        //connect
        if(strcmp(command, "connect") == 0){
            if(sockfd >= 0){
                printf("%d", sockfd);
                printf("Error: Already connected to server. Disconnect first.\n");
                continue;
            }
            
            char *hostname = strtok(NULL, " ");
            char* port_str = strtok(NULL, " ");

            if(hostname == NULL || port_str == NULL){
                fprintf(stderr, "Usage: connect <IP address> <port number>\n");
                continue;
            }
            int portno = atoi(port_str);

            struct sockaddr_in serv_addr;

            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if(sockfd < 0){
                error("ERROR opening socket");
            }
            bzero((char *)&serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(portno);

            // Convert IPv4 address from text to binary form
            if(inet_pton(AF_INET, hostname, &serv_addr.sin_addr) <= 0){

                //gethostbyname resolves hostname to IP address
                struct hostent *server = gethostbyname(hostname);
                if(server == NULL){
                    fprintf(stderr, "ERROR, no such host\n");
                    close(sockfd);
                    sockfd = -1;
                    continue;
                }
                bcopy((char *)server->h_addr_list[0], (char *)&serv_addr.sin_addr.s_addr, server->h_length);
            }

            if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
                printf("ERROR connecting");
                close(sockfd);
                sockfd = -1;
                continue;
            }
            else{
                printf("Successfuly connected to server %s:%d\n", inet_ntoa(serv_addr.sin_addr), ntohs(serv_addr.sin_port));
            }
        }

        //disconnect
        else if(strcmp(command, "disconnect") == 0){
            if(sockfd < 0){
                printf("Error: Not connected to any server.\n");
            }
            else{
                close(sockfd);
                sockfd = -1;
                printf("Disconnected from server\n");
            }
        }

        //exit
        else if(strcmp(command, "exit") == 0){
            if(sockfd >= 0){
                close(sockfd);
                sockfd = -1;
            }
            printf("Exiting client program.\n");
            break;
        }

        //create or update
        else if(strcmp(command, "create") == 0 || strcmp(command, "update") == 0){
            if(sockfd < 0){
                printf("Error: Not connected to any server. Use 'connect <IP address> <port number>'\n");
                continue;
            }

            char* key_str = strtok(NULL, " ");
            char* value_size_str = strtok(NULL, " ");

            if(key_str == NULL || value_size_str == NULL){
                fprintf(stderr, "Usage: %s <key> <value_size> <value>\n", command);
                continue;
            }
            int value_size = atoi(value_size_str);
            char *value = strtok(NULL, "");
            
            if(value == NULL || (int)strlen(value) != value_size){
                fprintf(stderr, "Error: Value size does not match the specified size of %d bytes\n", value_size);
                continue;
            }

            //send command
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, command);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //send key
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, key_str);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //send value size
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, value_size_str);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //send value
            int sent_bytes = 0;

            while(sent_bytes < value_size){
                int len_to_send;
                if(value_size - sent_bytes > MAX_BUFFER_SIZE - 1){
                    len_to_send = MAX_BUFFER_SIZE - 1;
                }
                else{
                    len_to_send = value_size - sent_bytes;
                }

                n = write(sockfd, value + sent_bytes, len_to_send);
                if(n < 0){
                    error("ERROR writing to socket");
                }
                sent_bytes += n;
            }

            //read response
            n = read(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            printf(">> %s\n", buffer);
        }

        //read
        else if(strcmp(command, "read") == 0){
            if(sockfd < 0){
                printf("Error: Not connected to any server. Use 'connect <IP address> <port number>'\n");
                continue;
            }

            char* key_str = strtok(NULL, " ");

            if(key_str == NULL){
                fprintf(stderr, "Usage: read <key>\n");
                continue;
            }

            //send command
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, command);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //send key
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, key_str);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //read response
            n = read(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            char *status = strtok(buffer, " ");
            if(strcmp(status, "ERROR") == 0){
                char *error_msg = strtok(NULL, "");
                printf(">> %s\n", error_msg);
                continue;
            }
            char* value_size_str = strtok(NULL, " ");
            
            int value_size = atoi(value_size_str);
            char *value = (char *)malloc(value_size + 1);

            int received_bytes = 0;

            while(received_bytes < value_size){
                int len_to_recieve;
                if(value_size - received_bytes > MAX_BUFFER_SIZE - 1){
                    len_to_recieve = MAX_BUFFER_SIZE - 1;
                }
                else{
                    len_to_recieve = value_size - received_bytes;
                }

                n = read(sockfd, value + received_bytes, len_to_recieve);
                if(n < 0){
                    error("ERROR reading from socket");
                }
                received_bytes += n;
            }
            value[value_size] = '\0';
            printf(">> Value: %s\n", value);
            free(value);
        }

        //delete
        else if(strcmp(command, "delete") == 0){
            if(sockfd < 0){
                printf("Error: Not connected to any server. Use 'connect <IP address> <port number>'\n");
                continue;
            }

            char* key_str = strtok(NULL, " ");

            if(key_str == NULL){
                fprintf(stderr, "Usage: delete <key>\n");
                continue;
            }

            //send command
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, command);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //send key
            bzero(buffer, MAX_BUFFER_SIZE);
            strcpy(buffer, key_str);
            n = write(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR writing to socket");
            }

            //read response
            n = read(sockfd, buffer, MAX_BUFFER_SIZE-1);
            if(n < 0){
                error("ERROR reading from socket");
            }
            buffer[n] = '\0';
            printf(">> %s\n", buffer);
        }

        //unknown command
        else{
            printf("Error: Unknown command '%s'\n", command);
        }
    }

    if(line){
        free(line);
    }
    if(input_stream != stdin){
        fclose(input_stream);
    }
    if(sockfd >= 0){
        close(sockfd);
        sockfd = -1;
    }
    return 0;

}