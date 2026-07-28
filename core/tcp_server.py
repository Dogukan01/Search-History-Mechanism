import asyncio

async def resp_parser(reader: asyncio.StreamReader):
    """
    Hem standart satır içi komutları (Inline) hem de orijinal
    Redis RESP (Redis Serialization Protocol) formatını okuyabilen parser.
    """
    while True:
        line = await reader.readline()
        if not line:
            break
            
        line_decoded = line.decode('utf-8', errors='ignore').strip()
        if not line_decoded:
            continue
            
        # Satır İçi (Inline) Komut (Örn: SET anahtar deger)
        if line_decoded[0] != '*':
            yield line_decoded.split()
            continue
            
        # RESP Dizi (Array) Formatı (Örn: *3\r\n$3\r\nSET...)
        try:
            num_args = int(line_decoded[1:])
            command_args = []
            for _ in range(num_args):
                arg_len_line = await reader.readline()
                arg_len_line = arg_len_line.decode('utf-8', errors='ignore').strip()
                if arg_len_line.startswith('$'):
                    arg_len = int(arg_len_line[1:])
                    if arg_len == -1:
                        command_args.append(None)
                    else:
                        # Tam olarak belirtilen byte kadar oku + \r\n
                        arg_data = await reader.readexactly(arg_len + 2)
                        command_args.append(arg_data[:-2].decode('utf-8', errors='ignore'))
            yield command_args
        except Exception:
            yield None # Hatalı format

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, db):
    """İstemciden gelen TCP komutlarını DB'ye yönlendirir."""
    addr = writer.get_extra_info('peername')
    print(f"[TCP] Yeni bağlantı: {addr}")
    
    try:
        async for command_args in resp_parser(reader):
            if not command_args:
                continue
                
            cmd = command_args[0].upper()
            
            # Yönlendirme Mekanizması
            if cmd == "PING":
                writer.write(b"+PONG\r\n")
                
            elif cmd == "COMMAND":
                # redis-cli bağlandığında ilk olarak hangi komutları desteklediğimizi sorar.
                # Şimdilik sadece OK diyerek geçiştiriyoruz.
                writer.write(b"+OK\r\n")
                
            elif cmd == "SET":
                if len(command_args) >= 3:
                    key = command_args[1]
                    val = command_args[2]
                    db.set(key, val)
                    
                    # SET key val EX 60 (TTL ile birlikte)
                    if len(command_args) == 5 and command_args[3].upper() == "EX":
                        db.expire(key, int(command_args[4]))
                        
                    writer.write(b"+OK\r\n")
                else:
                    writer.write(b"-ERR wrong number of arguments for 'set' command\r\n")
                    
            elif cmd == "GET":
                if len(command_args) == 2:
                    val = db.get(command_args[1])
                    if val is None:
                        writer.write(b"$-1\r\n") # RESP formatında NULL
                    else:
                        val_bytes = str(val).encode('utf-8')
                        writer.write(f"${len(val_bytes)}\r\n".encode() + val_bytes + b"\r\n")
                else:
                    writer.write(b"-ERR wrong number of arguments for 'get' command\r\n")
                    
            elif cmd == "INCR":
                if len(command_args) == 2:
                    val = db.incr(command_args[1])
                    writer.write(f":{val}\r\n".encode('utf-8'))
                else:
                    writer.write(b"-ERR wrong number of arguments for 'incr' command\r\n")
                    
            else:
                writer.write(f"-ERR unknown command '{cmd}'\r\n".encode('utf-8'))
                
            await writer.drain()
            
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        print(f"[TCP] Istemci hatasi: {e}")
    finally:
        print(f"[TCP] Baglanti koptu: {addr}")
        try:
            writer.close()
            await writer.wait_closed()
        except ConnectionResetError:
            pass

async def start_tcp_server(db, host='0.0.0.0', port=6379):
    """Saf TCP sunucusunu baslatir."""
    server = await asyncio.start_server(lambda r, w: handle_client(r, w, db), host, port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"[TCP] Orijinal Redis RESP TCP Server {addrs} uzerinde dinliyor...")
    
    async with server:
        await server.serve_forever()
