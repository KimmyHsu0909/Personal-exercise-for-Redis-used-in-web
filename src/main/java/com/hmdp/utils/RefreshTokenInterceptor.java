package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private  StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //从请求头获取token
        //HttpSession session = request.getSession();
        String token = request.getHeader("Authorization");

        if (StrUtil.isBlank(token)){
            //拦截
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return true;
        }

        //获取用户
        //Object user = session.getAttribute("user");
        Map<Object, Object> userMap= stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);

        //判断用户是否存在
        if (userMap.isEmpty()) {
            //拦截
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return true;
        }

        //将hash转为user对象
        UserDTO user = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), true);

        //保存到threadlocal
        UserHolder.saveUser(user);

        //刷新token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        HandlerInterceptor.super.afterCompletion(request, response, handler, ex);
    }
}
