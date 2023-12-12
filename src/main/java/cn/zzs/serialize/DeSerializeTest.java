package cn.zzs.serialize;

import cn.zzs.serialize.other.User;
import cn.zzs.serialize.other.UserService;
import com.alibaba.fastjson2.JSON;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.nustaq.serialization.FSTConfiguration;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * 反序列化性能对比测试
 *
 * @author zzs
 * @date 2020年11月6日 下午1:31:38
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DeSerializeTest {

    /**
     * 测试kryo反序列化，Kryo对象不是线程安全的，这里使用池获取
     *
     * @param kryoState
     * @return User
     * @throws Exception
     * @author zzs
     * @date 2020年11月8日 下午3:32:53
     */
    @Benchmark
    public User kryoDeSerialize(KryoState kryoState) throws Exception {
        ByteArrayInputStream byteArray = new ByteArrayInputStream(kryoState.bytes);
        Input input = new Input(byteArray);
        Kryo kryo = kryoState.kryoPool.obtain();
        User user = (User) kryo.readClassAndObject(input);
        kryoState.kryoPool.free(kryo);
        input.close();
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class KryoState {
        byte[] bytes;
        Pool<Kryo> kryoPool;

        @Setup(Level.Trial)
        public void prepare() throws Exception {
            kryoPool = new Pool<Kryo>(true, false, 16) {

                protected Kryo create() {
                    Kryo kryo = new Kryo();
                    // 不支持循环引用
                    kryo.setReferences(false);
                    // 禁止类注册
                    kryo.setRegistrationRequired(false);
                    // 设置实例化器
                    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
                    return kryo;
                }
            };
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            Output output = new ByteBufferOutput(byteArray);
            Kryo kryo = kryoPool.obtain();
            kryo.writeClassAndObject(output, new UserService().get());
            kryoPool.free(kryo);
            output.flush();
            output.close();
            bytes = byteArray.toByteArray();
            System.err.println("kryo serialized data size:" + bytes.length);
        }

    }

    /**
     * 测试fst反序列化，FSTConfiguration仅初始化一个实例，不重复创建
     *
     * @param fstState
     * @return User
     * @throws Exception
     * @author zzs
     * @date 2020年11月8日 下午3:34:08
     */
    @Benchmark
    public User fstDeSerialize(FSTState fstState) throws Exception {
        User user = (User) fstState.fSTConfiguration.asObject(fstState.bytes);
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class FSTState {

        byte[] bytes;
        FSTConfiguration fSTConfiguration;

        @Setup(Level.Trial)
        public void prepare() throws Exception {
            fSTConfiguration = FSTConfiguration.createDefaultConfiguration();
            //fSTConfiguration = FSTConfiguration.createJsonConfiguration();
            bytes = fSTConfiguration.asByteArray(new UserService().get());
            System.err.println("fst serialized data size:" + bytes.length);
        }

    }

    /**
     * 测试protostuff反序列化
     *
     * @param protostuffState
     * @return User
     * @throws Exception
     * @author zzs
     * @date 2020年11月8日 下午3:35:53
     */
    @Benchmark
    public User protostuffDeSerialize(ProtostuffState protostuffState) throws Exception {
        User user = new User();
        Schema<User> schema = (Schema<User>) RuntimeSchema.getSchema(User.class);
        ProtostuffIOUtil.mergeFrom(protostuffState.bytes, user, schema);
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class ProtostuffState {

        byte[] bytes;

        @Setup(Level.Trial)
        public void prepare() throws Exception {
            Schema<User> schema = (Schema<User>) RuntimeSchema.getSchema(User.class);
            bytes = ProtostuffIOUtil.toByteArray(new UserService().get(), schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
            System.err.println("protostuff serialized data size:" + bytes.length);
        }

    }

    /**
     * 测试jackson反序列化，ObjectMapper仅初始化一个实例，不重复创建
     *
     * @param jacksonState
     * @return User
     * @throws Exception
     * @author zzs
     * @date 2020年11月8日 下午3:29:17
     */
    @Benchmark
    public User jacksonDeSerialize(JacksonState jacksonState) throws Exception {
        User user = jacksonState.objectMapper.readValue(jacksonState.bytes, User.class);
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class JacksonState {

        byte[] bytes;
        ObjectMapper objectMapper;

        @Setup(Level.Trial)
        public void prepare() throws Exception {
            objectMapper = new ObjectMapper();
            bytes = objectMapper.writeValueAsBytes(new UserService().get());
            System.err.println("jackson serialized data size:" + bytes.length);
        }

    }

    /**
     * 测试fastjson反序列化
     *
     * @param fastJsonState
     * @return User
     * @author zzs
     * @date 2020年11月8日 下午3:28:53
     */
    @Benchmark
    public User fastJsonDeSerialize(FastJsonState fastJsonState) {
        User user = JSON.parseObject(fastJsonState.bytes, User.class);
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class FastJsonState {
        byte[] bytes;

        @Setup(Level.Trial)
        public void prepare() {
            bytes = JSON.toJSONBytes(new UserService().get());
            System.err.println("fastjson serialized data size:" + bytes.length);
        }

    }

    /**
     * 测试ObjectInputStream反序列化
     *
     * @param jdkState
     * @return User
     * @throws Exception
     * @author zzs
     * @date 2020年11月8日 下午3:30:36
     */
    @Benchmark
    public User jdkDeSerialize(JdkState jdkState) throws Exception {
        ByteArrayInputStream byteArray = new ByteArrayInputStream(jdkState.bytes);
        ObjectInputStream inputStream = new ObjectInputStream(byteArray);
        User user = (User) inputStream.readObject();
        inputStream.close();
        assert "zzs0".equals(user.getName());
        return user;
    }

    @State(Scope.Benchmark)
    public static class JdkState {
        byte[] bytes;

        @Setup(Level.Trial)
        public void prepare() throws Exception {
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(byteArray);
            outputStream.writeObject(new UserService().get());
            outputStream.flush();
            outputStream.close();
            bytes = byteArray.toByteArray();
            System.err.println("jdk serialized data size:" + bytes.length);
        }

    }

}
