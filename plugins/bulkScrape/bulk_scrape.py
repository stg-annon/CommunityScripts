import json
import sys
import time
from urllib.parse import urlparse
from types import SimpleNamespace

import log
from stash_interface import StashInterface

config = SimpleNamespace()

############################################################################
############################# CONFIG HERE ##################################
############################################################################

# Create missing performers/tags/studios
# Default: False (Prevent Stash from getting flooded with weird values)
config.create_missing_performers = False
config.create_missing_tags = True
config.create_missing_studios = True
config.create_missing_movies = False

# Delay between web requests
# Default: 5
config.delay = 5

# Name of the tag, that will be used for selecting scenes for bulk url scraping
config.bulk_url_control_tag = "scrape_bulk_url"

# Prefix of all fragment scraper tags
config.scrape_with_prefix = "scrape_with_"


############################################################################
############################################################################




def main():
	json_input = json.loads(sys.stdin.read())

	output = {}
	run(json_input, output)

	out = json.dumps(output)
	print(out + "\n")

def run(json_input, output):
	mode_arg = json_input['args']['mode']

	try:
		client = StashInterface(json_input["server_connection"])
		scraper = ScrapeController(client)
		
		if mode_arg == "create":
			scraper.add_tags()
		if mode_arg == "remove":
			scraper.remove_tags()

		if mode_arg == "url_scrape":
			scraper.bulk_url_scrape()
		if mode_arg == "movie_scrape":
			scraper.movie_scrape()
		if mode_arg == "fragment_scrape":
			scraper.scrape_scenes_with_fragment_tags()

	except Exception:
		raise

	output["output"] = "ok"


class ScrapeController:

	def __init__(self, client, create_missing_performers=False, create_missing_tags=False, create_missing_studios=False, create_missing_movies=False, delay=5):
		try:
			self.create_missing_movies = bool(config.create_missing_movies)
			self.create_missing_studios = bool(config.create_missing_studios)
			self.create_missing_tags = bool(config.create_missing_tags)
			self.create_missing_performers = bool(config.create_missing_performers)
			
			self.bulk_url_control_tag = str(config.bulk_url_control_tag)
			self.scrape_with_prefix = str(config.scrape_with_prefix)

			self.delay = int(config.delay)

			self.last_wait_time = -1
		except AttributeError as e:
			log.warning(e)
			log.warning("Using defaults for missing config values")
		except ValueError as e:
			log.warning(e)
			log.warning("Using defaults for wrong values")

		self.client = client

		log.info('######## Bulk Scraper ########')
		log.info(f'create_missing_performers: {self.create_missing_performers}')
		log.info(f'create_missing_tags: {self.create_missing_tags}')
		log.info(f'create_missing_studios: {self.create_missing_studios}')
		log.info(f'create_missing_movies: {self.create_missing_movies}')
		log.info(f'delay: {self.delay}')
		log.info('##############################')

	# Waits the remaining time between the last timestamp and the configured delay in seconds
	def wait(self):
		if self.delay:
			time_last = int(self.last_wait_time)
			time_now = int(time.time())
			if time_now > time_last:
				if time_now - time_last < self.delay:
					time.sleep(self.delay - (time_now - time_last) + 1)
			self.last_wait_time = time.time()


	def movie_scrape(self):

		movies = self.client.findMoviesMissingFrontImage()
		log.info(f'Found {len(movies)} movies with URLs')

		count = self.__movie_scrape(movies)
		log.info(f'Scraped data for {count} movies')

	def bulk_url_scrape(self):
		# Search for all scenes with scrape tag
		tag = self.client.findTagIdWithName(self.bulk_url_control_tag)
		if tag is None:
			sys.exit(f'Tag "{self.bulk_url_control_tag}" does not exist. Please create it via the "Create scrape tags" task')

		tag_ids = [tag]
		scenes = self.client.findScenesByTags(tag_ids)
		log.info(f'Found {len(scenes)} scenes with {self.bulk_url_control_tag} tag')
		count = self.__scrape_with_url(scenes)
		log.info(f'Scraped data for {count} scenes')
		return None

	def scrape_scenes_with_fragment_tags(self):
		tags=self.list_all_control_tags()
		for tag in tags:
			if tag.startswith("scrape_with_"):
				log.info(f"scraping all scenes with tag: {tag}")
				self.__scrape_with_tag(tag)


	def list_all_control_tags(self):
		scrapers = self.client.listSceneScrapers('FRAGMENT')
		scrapers = [f"{self.scrape_with_prefix}{s}" for s in scrapers]
		scrapers.append(self.bulk_url_control_tag)
		return scrapers

	def get_control_tag_ids(self):
		control_ids = list()
		for tag_name in self.list_all_control_tags():
			tag_id = self.client.findTagIdWithName(tag_name)
			if tag_id == None:
				continue
			control_ids.append(tag_id)
		return control_ids

	def add_tags(self):
		tags = self.list_all_control_tags()
		for tag_name in tags:
			tag_id = self.client.findTagIdWithName(tag_name)
			if tag_id == None:
				tag_id = self.client.createTagWithName(tag_name)
				log.info(f"adding tag {tag_name}")
			else:
				log.debug(f"tag exists, {tag_name}")

	def remove_tags(self):
		tags = self.list_all_control_tags()
		for tag_name in tags:
			tag_id = self.client.findTagIdWithName(tag_name)
			if tag_id == None:
				log.debug("Tag does not exist. Nothing to remove")
				continue
			log.info(f"Destroying tag {tag_name}")
			self.client.destroyTag(tag_id)


	def __scrape_with_url(self, scenes):
		last_request = -1
		if self.delay > 0:
			# Initialize last request with current time + delay time
			last_request = time.time() + self.delay

		working_scrapers = set()
		missing_scrapers = set()

		# Number of scraped scenes
		count = 0
		total = len(scenes)

		# Scrape if url not in missing_scrapers
		for i, scene in enumerate(scenes):
			# Update status bar
			log.progress(i/total)

			if scene.get('url') is None or scene.get('url') == "":
				log.info(f"Scene {scene.get('id')} is missing url")
				continue
			netloc = urlparse(scene.get("url")).netloc
			if netloc in working_scrapers or netloc not in missing_scrapers:
				log.info(f"Scraping URL for Scene {scene.get('id')}")
				self.wait()
				scraped_data = self.client.scrapeSceneURL(scene.get('url'))
				# If result is null, add url to missing_scrapers
				if scraped_data is None:
					log.warning(f"Missing scraper for {urlparse(scene.get('url')).netloc}")
					missing_scrapers.add(netloc)
					continue
				else:
					working_scrapers.add(netloc)
				# No data has been found for this scene
				if not any(scraped_data.values()):
					log.info(f"Could not get data for scene {scene.get('id')}")
					continue

				success = self.__update_scene_with_scrape_data(scene, scraped_data)
				if not success:
					log.warning(f"Failed to scrape scene {scene.get('id')}")

				log.debug(f"Scraped data for scene {scene.get('id')}")
				count += 1

		return count

	def __scrape_with_tag(self, tag):
		last_request = -1
		if self.delay > 0:
			# Initialize last request with current time + delay time
			last_request = time.time() + self.delay

		scenes = self.client.getScenesWithTag(tag)
		scraper_id = tag.replace(self.scrape_with_prefix,"")

		# Number of scraped scenes
		count = 0
		total = len(scenes)

		# Scrape if url not in missing_scrapers
		for i, scene in enumerate(scenes):
			# Update status bar
			log.progress(i/total)

			self.wait()
			scraped_data = self.client.runSceneScraper(scene, scraper_id)

			if scraped_data is None:
				log.info(f"Scraper ({scraper_id}) did not return a result for scene ({scene.get('id')}) ")
				continue
			else:
				# No data has been found for this scene
				if not any(scraped_data.values()):
					log.info(f"Could not get data for scene {scene.get('id')}")
					continue

				success = self.__update_scene_with_scrape_data(scene, scraped_data)
				if not success:
					log.warning(f"Failed to scrape scene {scene.get('id')}")

				count += 1

		return count

	def __update_scene_with_scrape_data(self, scene, scraped_data):
		# Create dict with scene data
		update_data = {
			'id': scene.get('id')
		}

		common_attrabutes = ['url','title','details','date']
		for c_attr in common_attrabutes:
			if scraped_data.get(c_attr):
				update_data[c_attr] = scraped_data.get(c_attr)

		if scraped_data.get('image'):
			update_data['cover_image'] = scraped_data.get('image')
		
		if scraped_data.get('tags'):
			tag_ids = list()
			for tag in scraped_data.get('tags'):
				if tag.get('stored_id'):
					tag_ids.append(tag.get('stored_id'))
				elif self.create_missing_tags and tag.get('name') != "":
					# Capitalize each word
					tag_name = " ".join(x.capitalize() for x in tag.get('name').split(" "))
					log.info(f'Create missing tag: {tag_name}')
					tag_ids.append(self.client.createTagWithName(tag_name))
			if len(tag_ids) > 0:
				update_data['tag_ids'] = tag_ids

		if scraped_data.get('performers'):
			performer_ids = list()
			for performer in scraped_data.get('performers'):
				if performer.get('stored_id'):
					performer_ids.append(performer.get('stored_id'))
				elif self.create_missing_performers and performer.get('name') != "":
					performer["name"] = " ".join(x.capitalize() for x in performer.get('name').split(" "))
					log.info(f'Create missing performer: {performer.get("name")}')
					performer_ids.append(self.client.createPerformer(performer))
			if len(performer_ids) > 0:
				update_data['performer_ids'] = performer_ids

		if scraped_data.get('studio'):
			log.debug(json.dumps(scraped_data.get('studio')))
			if dict_query(scraped_data, 'studio.stored_id'):
				update_data['studio_id'] = dict_query(scraped_data, 'studio.stored_id')
			elif self.create_missing_studios:
				studio = {}
				studio["name"] = " ".join(x.capitalize() for x in dict_query(scraped_data, 'studio.name').split(" "))
				studio["url"] = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(scene.get('url')))
				log.info(f'Creating missing studio {studio.get("name")}')
				update_data['studio_id'] = self.client.createStudio(studio)

		if scraped_data.get('movies') and update_data['studio_id']:
			movie_ids = list()
			for movie in scraped_data.get('movies'):
				if movie.get('stored_id'):
					movie_id = movie.get('stored_id')
					movie_ids.append( {'movie_id':movie_id, 'scene_index':None} )
				elif self.create_missing_movies and movie.get('name') != "":
					if update_data['studio_id']:
						movie['studio_id'] = update_data['studio_id']
					log.info(f'Create missing movie: "{movie.get("name")}"')
					movie_id = self.client.createMovieByName(movie)
					movie_ids.append( {'movie_id':movie_id, 'scene_index':None} )
			if len(movie_ids) > 0:
				update_data['movies'] = movie_ids

		log.debug('mapped scrape data to scene fields')

		# Only accept base64 images
		if update_data.get('cover_image') and not update_data.get('cover_image').startswith("data:image"):
			del update_data['cover_image']


		# Merge existing tags ignoring plugin control tags
		merged_tags = set()

		control_tag_ids = self.get_control_tag_ids()
		for tag in scene.get('tags'):
			if tag.get('id') not in control_tag_ids:
				merged_tags.add(tag.get('id'))
		if update_data.get('tag_ids'):
			merged_tags.update(update_data.get('tag_ids'))

		update_data['tag_ids'] = list(merged_tags)


		# Update scene with scraped scene data
		try:
			self.client.updateScene(update_data)
		except Exception as e:
			log.error('Error updating scene')
			log.error(json.dumps(update_data))
			log.error(str(e))

		return True

	def __movie_scrape(self, movies):
		last_request = -1
		if self.delay > 0:
			# Initialize last request with current time + delay time
			last_request = time.time() + self.delay

		missing_scrapers = list()

		# Number of scraped scenes
		count = 0

		total = len(movies)
		# Index for progress bar
		i = 0

		# Scrape if url not in missing_scrapers
		for movie in movies:
			# Update status bar
			i += 1
			log.progress(i/total)

			if movie.get('url') is None or movie.get('url') == "":
				log.info(f"Movie {movie.get('id')} is missing url")
				continue
			if urlparse(movie.get("url")).netloc not in missing_scrapers:
				log.debug(f"Scraping movie {movie.get('id')}")
				self.wait()
				scraped_data = self.client.scrapeMovieURL(movie.get('url'))
				# If result is null, add url to missing_scrapers
				if scraped_data is None:
					log.warning(f"Missing scraper for {urlparse(movie.get('url')).netloc}")
					missing_scrapers.append(urlparse(movie.get('url')).netloc)
					continue
				# No data has been found for this scene
				if not any(scraped_data.values()):
					log.info(f"Could not get data for scene {movie.get('id')}")
					continue


				# scraped_data keys(['name', 'aliases', 'duration', 'date', 'rating', 'director', 'url', 'synopsis', 'front_image', 'back_image', 'studio'])

				# Create dict with scene data
				update_data = {
					'id': movie.get('id')
				}


				if scraped_data.get('front_image'):
					update_data['front_image'] = scraped_data.get('front_image')
				if scraped_data.get('back_image'):
					update_data['back_image'] = scraped_data.get('back_image')

				if scraped_data.get('synopsis'):
					update_data['synopsis'] = scraped_data.get('synopsis')
				if scraped_data.get('date'):
					update_data['date'] = scraped_data.get('date')
				if scraped_data.get('aliases'):
					update_data['aliases'] = scraped_data.get('aliases')

				if scraped_data.get('studio'):
					update_data['studio_id'] = scraped_data.get('studio').get('id')

				# # Update scene with scraped scene data

				try:
					self.client.updateMovie(update_data)
				except Exception as e:
					log.error('update error')
				count += 1

		return count


# simple function to address large nested python dicts with dot notation 
def dict_query(dictIn, query, default=None):
		if not isinstance(dictIn, dict):
			raise TypeError(f"dict_query expects python dict received {type(dictIn)}")
		
		keys = query.split(".")
		val = None

		for key in keys:
			if val:
				if isinstance(val, list):
					val = [ v.get(key, default) if v else None for v in val]
				else:
					val = val.get(key, default)
			else:
				val = dict.get(dictIn, key, default)

			if not val:
				break;

		return val

main()
