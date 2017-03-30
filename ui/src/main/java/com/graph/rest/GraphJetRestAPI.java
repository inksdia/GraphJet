package com.graph.rest;

import com.google.gson.Gson;
import com.graph.GraphHelper;
import com.graph.Impl.GraphJetServiceImpl;
import com.graph.Utils.Preconditions;
import com.graph.service.GraphJetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Created by saurav on 29/03/17.
 */
@Path("/graph")
@Component
public class GraphJetRestAPI {

    private final Logger logger = LoggerFactory.getLogger(GraphJetRestAPI.class);
    final Gson gson = new Gson();

    private GraphJetService graphJetService = new GraphJetServiceImpl();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response welcome() {
        logger.debug("for testing REST API");
        return generateResponse("Welcome");
    }

    @POST
    @Path("{identifier}/create")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createGraph(@PathParam("identifier") final String identifer) {
        logger.debug("Request to create new Graph");
        return generateResponse(graphJetService.createGraph(identifer));
    }

    @POST
    @Path("{identifier}/insert/")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insertEdge(@PathParam("identifier") final String identifer, final IngestMessageDTO dto) throws IOException {
        //For testing
        for (int i = 0; i < 100000; i += 10000) {
            final IngestMessageDTO ingestMessageDTO = new IngestMessageDTO();
            try {
                ingestMessageDTO.setMessages(GraphHelper.getMessages(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.debug("Post call for Edge Insertion" + ingestMessageDTO);
            Preconditions.notNull(ingestMessageDTO, "EdgeDto can't be null");
            graphJetService.insertEdge(identifer, ingestMessageDTO);
        }

        /*new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100000; i += 10000) {
                    final IngestMessageDTO ingestMessageDTO = new IngestMessageDTO();
                    try {
                        ingestMessageDTO.setMessages(GraphHelper.getMessages(i));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    logger.debug("Post call for Edge Insertion" + ingestMessageDTO);
                    Preconditions.notNull(ingestMessageDTO, "EdgeDto can't be null");
                    graphJetService.insertEdge(ingestMessageDTO);
                }
            }
        }).start();*//*new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100000; i += 10000) {
                    final IngestMessageDTO ingestMessageDTO = new IngestMessageDTO();
                    try {
                        ingestMessageDTO.setMessages(GraphHelper.getMessages(i));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    logger.debug("Post call for Edge Insertion" + ingestMessageDTO);
                    Preconditions.notNull(ingestMessageDTO, "EdgeDto can't be null");
                    graphJetService.insertEdge(ingestMessageDTO);
                }
            }
        }).start();*/
        return generateResponse("Graph creation started");
    }

    @GET
    @Path("{identifier}/topUsers")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUsers(@PathParam("identifier") final String identifer, @QueryParam("count") int count) {
        return generateResponse(graphJetService.topUsers(identifer, count));
    }

    @GET
    @Path("{identifier}/topMessagesByUserId/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topMessage(@PathParam("identifier") final String identifer, @PathParam("userId") Long userId, @QueryParam("count") int count) {
        logger.debug("Request for top Messages");
        return generateResponse(graphJetService.topMessagesByUserId(identifer, userId, count));
    }

    @GET
    @Path("{identifier}/topHashtags")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topHashtags(@PathParam("identifier") final String identifer, @QueryParam("count") int count) {
        logger.debug("Request for top Hashtags");
        return generateResponse(graphJetService.topHashTags(identifer, count));
    }

    @GET
    @Path("{identifier}/topUserMessages/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topUserMessage(@PathParam("identifier") final String identifer, @PathParam("userId") Long userId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " tweets by " + userId);
        return generateResponse(graphJetService.topMessagesByUserId(identifer, userId, count));
    }

    @GET
    @Path("{identifier}/topMessageUsers/{msgId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topTweetsUser(@PathParam("identifier") final String identifer, @PathParam("msgId") Long msgId, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " user related/connected to " + msgId + "tweets");
        return generateResponse(graphJetService.topUsersByMsgId(identifer, msgId, count));
    }

    @GET
    @Path("{identifier}/similarHashTags/{hashTag}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response similarHashTags(@PathParam("identifier") final String identifer, @PathParam("hashTag") String hashTag, @QueryParam("count") int count) {
        logger.debug("Request for similar " + count + " hashtags for hashtag" + hashTag);
        Preconditions.notBlank(hashTag);
        return generateResponse(graphJetService.similarHashTags(identifer, hashTag, count));
    }

    @GET
    @Path("{identifier}/topHashTagMessages/{hashTag}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topHashTagMessages(@PathParam("identifier") final String identifer, @PathParam("hashTag") String hashTag, @QueryParam("count") int count) {
        logger.debug("Request for similar " + count + " hashtags for hashtag" + hashTag);
        Preconditions.notBlank(hashTag);
        Long hashTagId = Integer.toUnsignedLong(hashTag.hashCode());
        return generateResponse(graphJetService.topMessagesByHashTags(identifer, hashTagId, count));
    }

    @GET
    @Path("{identifier}/topMessages")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topTweets(@PathParam("identifier") final String identifer, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " users");
        return generateResponse(graphJetService.topMessages(identifer, count));
    }

    @GET
    @Path("{identifier}/topInfluencers")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response topInfluencers(@PathParam("identifier") final String identifer, @QueryParam("count") int count) {
        logger.debug("Request for top " + count + " users");
        return generateResponse(graphJetService.topInfluencers(identifer, count));
    }


    private <T> Response generateResponse(T obj) {
        return Response.ok(gson.toJson(obj)).build();
    }

}
