import requests
import sys
import log
import re


class StashInterface:
    port = ""
    url = ""
    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "keep-alive",
        "DNT": "1"
    }
    cookies = {}

    def __init__(self, conn, fragments={}):
        self.port = conn['Port']
        scheme = conn['Scheme']

        # Session cookie for authentication
        self.cookies = {
            'session': conn.get('SessionCookie').get('Value')
        }

        domain = conn.get('Domain') if conn.get('Domain') else 'localhost'

        # Stash GraphQL endpoint
        self.url = scheme + "://" + domain + ":" + str(self.port) + "/graphql"
        log.debug(f"Using stash GraphQl endpoint at {self.url}")

        self.fragments = fragments
        self.fragments.update(stash_gql_fragments)

    def __resolveFragments(self, query):

        fragmentRefrences = list(set(re.findall(r'(?<=\.\.\.)\w+', query)))
        fragments = []
        for ref in fragmentRefrences:
            fragments.append({
                "fragment": ref,
                "defined": bool(re.search("fragment {}".format(ref), query))
            })

        if all([f["defined"] for f in fragments]):
            return query
        else:
            for fragment in [f["fragment"] for f in fragments if not f["defined"]]:
                if fragment not in self.fragments:
                    raise Exception(f'GraphQL error: fragment "{fragment}" not defined')
                query += self.fragments[fragment]
            return self.__resolveFragments(query)

    def __callGraphQL(self, query, variables=None):

        query = self.__resolveFragments(query)

        json = {'query': query}
        if variables is not None:
            json['variables'] = variables

        response = requests.post(self.url, json=json, headers=self.headers, cookies=self.cookies)

        if response.status_code == 200:
            result = response.json()
            if result.get("error", None):
                for error in result["error"]["errors"]:
                    raise Exception("GraphQL error: {}".format(error))
            if result.get("data", None):
                return result.get("data")
        elif response.status_code == 401:
            sys.exit("HTTP Error 401, Unauthorised. Cookie authentication most likely failed")
        else:
            raise ConnectionError(
                "GraphQL query failed:{} - {}. Query: {}. Variables: {}".format(
                    response.status_code, response.content, query, variables)
            )

    def scan_for_new_files(self):
        try:
            query = """
                    mutation {
                        metadataScan (
                            input: {
                                useFileMetadata: true 
                                scanGenerateSprites: false
                                scanGeneratePreviews: false
                                scanGenerateImagePreviews: false
                                stripFileExtension: false
                            }
                        ) 
                    }
            """
            result = self.__callGraphQL(query)
        except ConnectionError:
            query = """
                    mutation {
                        metadataScan (
                            input: {
                                useFileMetadata: true
                            }
                        ) 
                    }
            """
            result = self.__callGraphQL(query)
        log.debug("ScanResult" + str(result))

    def findTagIdWithName(self, name):
        query = """
            query {
                allTags {
                id
                name
                }
            }
        """

        result = self.__callGraphQL(query)

        for tag in result["allTags"]:
            if tag["name"] == name:
                return tag["id"]
        return None

    def createTagWithName(self, name):
        query = """
            mutation tagCreate($input:TagCreateInput!) {
                tagCreate(input: $input){
                    id
                }
            }
        """
        variables = {'input': {
            'name': name
        }}

        result = self.__callGraphQL(query, variables)
        return result["tagCreate"]["id"]

    def destroyTag(self, tag_id):
        query = """
            mutation tagDestroy($input: TagDestroyInput!) {
                tagDestroy(input: $input)
            }
        """
        variables = {'input': {
            'id': tag_id
        }}

        self.__callGraphQL(query, variables)

    def getSceneById(self, scene_id):
        query = """
            query findScene($id: ID!) {
                findScene(id: $id) {
                    id
                    title
                    details
                    url
                    date
                    rating
                    galleries {
                        id
                    }
                    studio {
                        id
                    }
                    tags {
                        id
                    }
                    performers {
                        id
                    }
                }
            }
        """

        variables = {
            "id": scene_id
        }

        result = self.__callGraphQL(query, variables)

        return result.get('findScene')

    def findRandomSceneId(self):
        query = """
            query findScenes($filter: FindFilterType!) {
                findScenes(filter: $filter) {
                    count
                    scenes {
                        id
                        tags {
                            id
                        }
                    }
                }
            }
        """

        variables = {'filter': {
            'per_page': 1,
            'sort': 'random'
        }}

        result = self.__callGraphQL(query, variables)

        if result["findScenes"]["count"] == 0:
            return None

        return result["findScenes"]["scenes"][0]

    # This method wipes rating, tags, performers, gallery and movie if omitted
    def updateScene(self, scene_data):
        query = """
            mutation sceneUpdate($input:SceneUpdateInput!) {
                sceneUpdate(input: $input) {
                    id
                }
            }
        """
        variables = {'input': scene_data}

        result = self.__callGraphQL(query, variables)
        return result["sceneUpdate"]["id"]

    def updateGallery(self, gallery_data):
        query = """
            mutation galleryUpdate($input: GalleryUpdateInput!) {
                galleryUpdate(input: $input) {
                    id
                }
            }
        """

        variables = {'input': gallery_data}

        result = self.__callGraphQL(query, variables)
        return result["galleryUpdate"]["id"]

    def updateImage(self, image_data):
        query = """
            mutation($input: ImageUpdateInput!) {
                imageUpdate(input: $input) {
                    id
                }
            }
        """

        variables = {'input': image_data}

        result = self.__callGraphQL(query, variables)
        return result["imageUpdate"]["id"]


    def createPerformer(self, performer_data):
        name = performer_data.get("name")
        query = """
            mutation($name: String!) {
                performerCreate(input: { name: $name }) {
                    id
                }
            }
        """

        variables = {
            'name': name
        }

        result = self.__callGraphQL(query, variables)
        performer_data["id"] = result.get('performerCreate').get('id')

        return self.updatePerformer(performer_data)
    def updatePerformer(self, performer_data):
        query = """
            mutation performerUpdate($input:PerformerUpdateInput!) {
                performerUpdate(input: $input) {
                    id
                }
            }
        """
        variables = {'input': performer_data}

        result = self.__callGraphQL(query, variables)
        return result["performerUpdate"]["id"]

    def findOrCreateMovie(self, movie_data):
        search = self.findMovieByName(movie_data.get('name'))
        if search:
            for key in search.keys():
                if key in movie_data:
                    search[key] = movie_data[key]
            return self.updateMovie(search)
        else:
            return self.createMovieByName(movie_data)

    def createMovieByName(self, movie_data):
        name = movie_data.get("name")
        query = """
            mutation($name: String!) {
                movieCreate(input: { name: $name }) {
                    id
                }
            }
        """

        variables = {
            'name': name
        }

        result = self.__callGraphQL(query, variables)
        movie_data["id"] = result.get('movieCreate').get('id')

        return self.updateMovie(movie_data)
    def updateMovie(self, movie_data):
        query = """
            mutation MovieUpdate($input:MovieUpdateInput!) {
                movieUpdate(input: $input) {
                    id
                }
            }
        """
        variables = {'input': movie_data}

        result = self.__callGraphQL(query, variables)
        return result["movieUpdate"]["id"]

    def createStudio(self, studio_data):
        name = studio_data.get("name")
        query = """
            mutation($name: String!) {
                studioCreate(input: { name: $name }) {
                    id
                }
            }
        """
        variables = {
            'name': name
        }

        result = self.__callGraphQL(query, variables)
        studio_data["id"] = result.get("studioCreate").get("id")

        return self.updateStudio(studio_data)
    def updateStudio(self, studio_data):
        query = """
            mutation StudioUpdate($input:StudioUpdateInput!) {
                studioUpdate(input: $input) {
                    id
                }
            }
        """
        variables = {'input': studio_data}

        result = self.__callGraphQL(query, variables)
        return result["studioUpdate"]["id"]


    # Returns all scenes for the given regex
    def findScenesByPathRegex(self, regex):
        return self.__findScenesByPathRegex(regex)

    # Returns all scenes for the given regex
    # Searches all pages from given page on (default: 1)
    def __findScenesByPathRegex(self, regex, page=1):
        query = """
            query findScenesByPathRegex($filter: FindFilterType!) {
                findScenesByPathRegex(filter:$filter)  {
                    count
                    scenes {
                        title
                        id
                        url
                        rating
                        galleries {id}
                        studio {id}
                        tags {id}
                        performers {id}
                        path
                    }
                }
            }
        """

        variables = {
            "filter": {
                "q": regex,
                "per_page": 100,
                "page": page
            }
        }

        result = self.__callGraphQL(query, variables)
        log.debug(f"Regex found {result.get('findScenesByPathRegex').get('count')} scene(s) on page {page}")

        scenes = result.get('findScenesByPathRegex').get('scenes')

        # If page is full, also scan next page:
        if len(scenes) == 100:
            next_page = self.__findScenesByPathRegex(regex, page + 1)
            for scene in next_page:
                scenes.append(scene)

        if page == 1:
            log.debug(f"Regex found a total of {len(scenes)} scene(s)")
        return scenes

    def findGalleriesByTags(self, tag_ids):
        return self.__findGalleriesByTags(tag_ids)

    # Searches for galleries with given tags
    # Requires a list of tagIds
    def __findGalleriesByTags(self, tag_ids, page=1):
        query = """
        query findGalleriesByTags($tags: [ID!], $page: Int) {
            findGalleries(
                gallery_filter: { tags: { value: $tags, modifier: INCLUDES_ALL } }
                filter: { per_page: 100, page: $page }
            ) {
                count
                galleries {
                    id
                    scenes {
                        id
                    }
                }
            }
        }
        """

        variables = {
            "tags": tag_ids,
            "page": page
        }

        result = self.__callGraphQL(query, variables)

        galleries = result.get('findGalleries').get('galleries')

        # If page is full, also scan next page(s) recursively:
        if len(galleries) == 100:
            next_page = self.__findGalleriesByTags(tag_ids, page + 1)
            for gallery in next_page:
                galleries.append(gallery)

        return galleries

    def findGalleries(self, gallery_filter=None):
        return self.__findGalleries(gallery_filter)

    def __findGalleries(self, gallery_filter=None, page=1):
        per_page = 100
        query = """
            query($studio_ids: [ID!], $page: Int, $per_page: Int) {
                findGalleries(
                    gallery_filter: { studios: { modifier: INCLUDES, value: $studio_ids } }
                    filter: { per_page: $per_page, page: $page }
                ) {
                    count
                    galleries {
                        id
                        studio {id}
                    }
                }
            }
        """

        variables = {
            "page": page,
            "per_page": per_page
        }
        if gallery_filter:
            variables['gallery_filter'] = gallery_filter

        result = self.__callGraphQL(query, variables)

        galleries = result.get('findGalleries').get('galleries')

        # If page is full, also scan next page(s) recursively:
        if len(galleries) == per_page:
            next_page = self.__findGalleries(gallery_filter, page + 1)
            for gallery in next_page:
                galleries.append(gallery)

        return galleries

    def findImages(self, image_filter=None):
        return self.__findImages(image_filter)

    def __findImages(self, image_filter=None, page=1):
        per_page = 1000
        query = """
        query($per_page: Int, $page: Int, $image_filter: ImageFilterType) {
            findImages(image_filter: $image_filter ,filter: { per_page: $per_page, page: $page }) {
                count
                images {
                    id
                    title
                    studio {
                        id
                    }
                    performers {
                        id
                    }
                    tags {
                        id
                    }
                    rating
                    galleries {
                        id
                    }
                }
            }
        }
        """

        variables = {
            'per_page': per_page,
            'page': page
        }
        if image_filter:
            variables['image_filter'] = image_filter

        result = self.__callGraphQL(query, variables)

        images = result.get('findImages').get('images')

        if len(images) == per_page:
            next_page = self.__findImages(image_filter, page + 1)
            for image in next_page:
                images.append(image)

        return images

    def updateImageStudio(self, image_ids, studio_id):
        query = """
        mutation($ids: [ID!], $studio_id: ID) {
            bulkImageUpdate(input: { ids: $ids, studio_id: $studio_id }) {
                id
            }
        }
        """

        variables = {
            "ids": image_ids,
            "studio_id": studio_id
        }

        self.__callGraphQL(query, variables)

    def findScenesByTags(self, tag_ids):
        return self.__findScenesByTags(tag_ids)

    def __findScenesByTags(self, tag_ids):
        query = """
        query($tags: [ID!]) {
            findScenes(
                scene_filter: { tags: { modifier: INCLUDES_ALL, value: $tags } }
                filter: { per_page: -1 }
            ) {
                count
                scenes {
                    ...stashScene
                }
            }
        }
        """

        variables = {
            "tags": tag_ids
        }

        result = self.__callGraphQL(query, variables)
        scenes = result.get('findScenes').get('scenes')

        return scenes

    # Scrape
    def scrapeSceneURL(self, url):
        query = """
            query($url: String!) {
                scrapeSceneURL(url: $url) {
                    ...scrapedScene
                }
            }
        """

        variables = {
            'url': url
        }

        result = self.__callGraphQL(query, variables)
        return result.get('scrapeSceneURL')

    def scrapeMovieURL(self, url):
        query = """
            query($url: String!) {
                scrapeMovieURL(url: $url) {
                  name
                  aliases
                  duration
                  date
                  rating
                  director
                  url
                  synopsis
                  front_image
                  back_image
                  studio {
                    id
                    name
                  }
                }
            }
        """

        variables = {
            'url': url
        }

        result = self.__callGraphQL(query, variables)
        return result.get('scrapeMovieURL')

    def createSceneMarker(self, seconds, scene_id, primary_tag_id, title="", tag_ids=[]):
        query = """
            mutation SceneMarkerCreate($title: String!, $seconds: Float!, $scene_id: ID!, $primary_tag_id: ID!, $tag_ids: [ID!] = []) {
              sceneMarkerCreate(
                input: {title: $title, seconds: $seconds, scene_id: $scene_id, primary_tag_id: $primary_tag_id, tag_ids: $tag_ids}
              ) {
                id
                __typename
              }
            }
        """
        variables = {
          "tag_ids": tag_ids,
          "title": title,
          "seconds": seconds,
          "scene_id": scene_id,
          "primary_tag_id": primary_tag_id
        }





    def findMovieByName(self, name):
        query = "query {allMovies {id name aliases date rating studio {id name} director synopsis}}"

        response = self.__callGraphQL(query)

        for movie in response.get('allMovies'):
            if movie.get('name') == name:
                return movie
        return None

    def findMoviesByUrl(self):
        query = "query {allMovies {id name aliases url synopsis scene_count}}"

        movies = []

        response = self.__callGraphQL(query)
        for movie in response.get('allMovies'):
            if movie.get('url') and not movie.get('date'):
                movies.append(movie)
        return movies

    def findMoviesMissingFrontImage(self):
        query = """
            query { findMovies( movie_filter: {is_missing: "front_image"} filter: {per_page:-1}) {
                count
                movies {id name date aliases url rating scene_count studio {id name} url director synopsis}}
            }
        """

        response = self.__callGraphQL(query)
        return response.get('findMovies').get('movies')

    def findScenesWherePathLike(self, pathPart):

        query = """
            query FindScenes($partial_path: String!) {
              findScenes(
                filter: {per_page:-1},
                scene_filter: { path:{ value:$partial_path, modifier:INCLUDES }}
              ) {
                count
                scenes{
                  title
                  date
                  path
                  scene_markers {
                    primary_tag {id, name}
                    seconds
                    __typename
                  }
                }
                __typename
              }
            }
        """


        variables = {
            'partial_path': pathPart
        }

        result = self.__callGraphQL(query, variables)
        return result.get('findScene').get('scenes')


    def findMarkersBySceneId(self, sceneID):
        query = """
            query { findScene(id: $sceneID) {
                title
                date
                scene_markers {
                  primary_tag {id, name}
                  seconds
                  __typename
                }
              }
            }
        """

        variables = {
            'sceneID': sceneID
        }

        result = self.__callGraphQL(query, variables)
        return result.get('findScene').get('scene_markers')

    def listSceneScrapers(self, type):
        query = """
        query listSceneScrapers {
            listSceneScrapers {
              id
              name
              scene{
                supported_scrapes
              }
            }
          }
        """
        ret = []
        result = self.__callGraphQL(query)
        for r in result["listSceneScrapers"]:
            if type in r["scene"]["supported_scrapes"]:
                ret.append(r["id"])
        return ret

    def listSceneFragmentScrapers(self, type):
        query = """
        query listSceneScrapers {
            listSceneScrapers {
              id
              name
              scene{
                supported_scrapes
              }
            }
          }
        """
        ret = []
        result = self.__callGraphQL(query)
        for r in result["listSceneScrapers"]:
            if type in r["scene"]["supported_scrapes"]:
                ret.append(r["id"])
        return ret

    def getScenesWithTag(self, tag):
        tagID = self.findTagIdWithName(tag)
        query = """query findScenes($scene_filter: SceneFilterType!) {
          findScenes(
           scene_filter: $scene_filter
           filter: {per_page: -1}
          ) {
            count
            scenes {
              ...stashScene
            }
          }
        }
      """

        variables = {"scene_filter": {"tags": {"value": [tagID], "modifier": "INCLUDES"}}}
        result = self.__callGraphQL(query, variables)
        return result["findScenes"]["scenes"]

    def runSceneScraper(self, scene, scraper):
        query = """query ScrapeScene($scraper_id: ID!, $scene: SceneUpdateInput!) {
           scrapeScene(scraper_id: $scraper_id, scene: $scene) {
              ...scrapedScene
            }
          }
        """
        variables = {"scraper_id": scraper,
                     "scene": {"title": scene["title"], "date": scene["date"], "details": scene["details"],
                               "gallery_ids": [], "id": scene["id"], "movies": None, "performer_ids": [],
                               "rating": scene["rating"], "stash_ids": scene["stash_ids"], "studio_id": None,
                               "tag_ids": None, "url": scene["url"]}}
        result = self.__callGraphQL(query, variables)
        return result["scrapeScene"]

stash_gql_fragments = {
    "scrapedScene":"""
        fragment scrapedScene on ScrapedScene {
          title
          details
          url
          date
          image
          file{
            size
            duration
            video_codec
            audio_codec
            width
            height
            framerate
            bitrate
          }
          studio{
            stored_id
            name
            url
            remote_site_id
          }
          tags{ ...scrapedSceneTag }
          performers{
            ...scrapedScenePerformer
          }
          movies{
            ...scrapedSceneMovie
          }
          remote_site_id
          duration
          fingerprints{
            algorithm
            hash
            duration
          }
          __typename
        }
    """,
    "scrapedScenePerformer":"""
        fragment scrapedScenePerformer on ScrapedScenePerformer {
          stored_id
          name
          gender
          url
          twitter
          instagram
          birthdate
          ethnicity
          country
          eye_color
          height
          measurements
          fake_tits
          career_length
          tattoos
          piercings
          aliases
          tags { ...scrapedSceneTag }
          remote_site_id
          images
          details
          death_date
          hair_color
          weight
          __typename
        }
    """,
    "scrapedSceneTag": """
        fragment scrapedSceneTag on ScrapedSceneTag {
            stored_id
            name
        }
    """,
    "scrapedSceneMovie": """
        fragment scrapedSceneMovie on ScrapedSceneMovie {
            stored_id
            name
            aliases
            duration
            date
            rating
            director
            synopsis
            url
        }
    """,
    "scrapedPerformer":"""
        fragment scrapedPerformer on ScrapedPerformer {
            name
            gender
            url
            twitter
            instagram
            birthdate
            ethnicity
            country
            eye_color
            height
            measurements
            fake_tits
            career_length
            tattoos
            piercings
            aliases
            tags { ...scrapedSceneTag }
            image
            details
            favorite
            death_date
            hair_color
            weight
            __typename
        }
    """,
    "stashSceneUpdate":"""
        fragment stashSceneExit on Scene {
            id
            title
            details
            url
            date
            rating
            gallery_ids
            studio_id
            performer_ids
            movies
            tag_ids
            stash_ids
        }
    """,
    "stashScene":"""
        fragment stashScene on Scene {
          id
          checksum
          oshash
          title
          details
          url
          date
          rating
          organized
          o_counter
          path
          tags {
            ...stashTag
          }
          file {
            size
            duration
            video_codec
            audio_codec
            width
            height
            framerate
            bitrate
          }
          galleries {
            id
            checksum
            path
            title
            url
            date
            details
            rating
            organized
            studio {
              id
              name
              url
            }
            image_count
            tags {
              ...stashTag
            }
          }
          performers {
            ...stashPerformer
          }
          studio{
            id
            name
            url
            stash_ids{
                endpoint
                stash_id
            }
          }
          stash_ids{
            endpoint
            stash_id
          }
        }
    """,
    "stashPerformer":"""
        fragment stashPerformer on Performer {
            id
            checksum
            name
            url
            gender
            twitter
            instagram
            birthdate
            ethnicity
            country
            eye_color
            height
            measurements
            fake_tits
            career_length
            tattoos
            piercings
            aliases
            favorite
            tags { ...stashTag }
            image_path
            scene_count
            image_count
            gallery_count
            stash_ids {
                stash_id
                endpoint
                __typename
            }
            rating
            details
            death_date
            hair_color
            weight
            __typename
        }
    """,
    "stashSceneMarker":"""
        fragment stashSceneMarker on SceneMarker {
            id
            scene
            title
            seconds
            primary_tag { ...stashTag }
            tags {...stashTag }
        }
    """,
    "stashTag":"""
        fragment stashTag on Tag {
            id
            name
            image_path
            scene_count
        }
    """
}