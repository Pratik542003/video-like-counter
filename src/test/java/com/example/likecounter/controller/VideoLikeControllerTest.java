package com.example.likecounter.controller;

import com.example.likecounter.exception.GlobalExceptionHandler;
import com.example.likecounter.service.VideoLikeService;
import com.example.likecounter.dto.LikeCountResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(VideoLikeController.class)
@org.springframework.context.annotation.Import(GlobalExceptionHandler.class)
class VideoLikeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private VideoLikeService service;

    @Test
    void postLikes_returnsUpdatedCount() throws Exception {
        when(service.incrementLikes(1L)).thenReturn(new LikeCountResponse(1L, 1L));

        mockMvc.perform(post("/api/videos/1/likes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.videoId").value(1))
                .andExpect(jsonPath("$.likeCount").value(1));
    }

    @Test
    void getLikes_returnsCurrentCount() throws Exception {
        when(service.getLikes(1L)).thenReturn(new LikeCountResponse(1L, 7L));

        mockMvc.perform(get("/api/videos/1/likes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.likeCount").value(7));
    }

    @Test
    void deleteLikes_returnsDecrementedCount() throws Exception {
        when(service.decrementLikes(1L)).thenReturn(new LikeCountResponse(1L, 0L));

        mockMvc.perform(delete("/api/videos/1/likes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.likeCount").value(0));
    }

    @Test
    void invalidVideoId_returnsBadRequest() throws Exception {
        mockMvc.perform(get("/api/videos/not-a-number/likes"))
                .andExpect(status().isBadRequest());
    }
}
