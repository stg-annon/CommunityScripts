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

# url scrape config
config.bulk_url_scrape_scenes = True
config.bulk_url_scrape_galleries = True
config.bulk_url_scrape_movies = True
config.bulk_url_scrape_performers = False

# fragment scrape config
config.fragment_scrape_scenes = True
config.fragment_scrape_galleries = True
config.fragment_scrape_movies = True
config.fragment_scrape_performers = False

# Delay between web requests
# Default: 5
config.delay = 5

# Name of the tag, that will be used for selecting scenes for bulk url scraping
config.bulk_url_control_tag = "blk_scrape_url"

# Prefix of all fragment scraper tags
config.scrape_with_prefix = "blk_scrape_"


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
		if mode_arg == "fragment_scrape":
			scraper.bulk_fragment_scrape()

	except Exception:
		raise

	output["output"] = "ok"


class ScrapeController:

	def __init__(self, client, create_missing_performers=False, create_missing_tags=False, create_missing_studios=False, create_missing_movies=False, delay=5):
		try:
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

		self.client.reload_scrapers()

		log.info('######## Bulk Scraper ########')
		log.info(f'create_missing_performers: {config.create_missing_performers}')
		log.info(f'create_missing_tags: {config.create_missing_tags}')
		log.info(f'create_missing_studios: {config.create_missing_studios}')
		log.info(f'create_missing_movies: {config.create_missing_movies}')
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


	def add_tags(self):
		tags = self.list_all_control_tags()
		for tag_name in tags:
			tag_id = self.client.get_tag_id_from_name(tag_name)
			if tag_id == None:
				tag_id = self.client.create_tag({'name':tag_name})
				log.info(f"adding tag {tag_name}")
			else:
				log.debug(f"tag exists, {tag_name}")

	def remove_tags(self):
		tags = self.list_all_control_tags()
		for tag_name in tags:
			tag_id = self.client.get_tag_id_from_name(tag_name)
			if tag_id == None:
				log.debug("Tag does not exist. Nothing to remove")
				continue
			log.info(f"Destroying tag {tag_name}")
			self.client.destroy_tag(tag_id)

	def bulk_url_scrape(self):
		# Scrape Everything enabled in config
		tag_id = self.client.get_tag_id_from_name(self.bulk_url_control_tag)
		if tag_id is None:
			sys.exit(f'Tag "{self.bulk_url_control_tag}" does not exist. Please create it via the "Create scrape tags" task')

		if config.bulk_url_scrape_scenes:
			scenes = self.client.find_scenes(f={
				"tags": {
					"value": [tag_id],
					"modifier": "INCLUDES"
				},
				"url": {
					"value": "",
					"modifier": "NOT_NULL"
				}
			})

			log.info(f'Found {len(scenes)} scenes with {self.bulk_url_control_tag} tag')
			count = self.__scrape_scenes_with_url(scenes)
			log.info(f'Scraped data for {count} scenes')
			log.info('##############################')

		if config.bulk_url_scrape_galleries:
			galleries = self.client.find_galleries(f={
				"tags": {
					"value": [tag_id],
					"modifier": "INCLUDES"
				},
				"url": {
					"value": "",
					"modifier": "NOT_NULL"
				}
			})

			log.info(f'Found {len(galleries)} galleries with {self.bulk_url_control_tag} tag')
			count = self.__scrape_galleries_with_url(galleries)
			log.info(f'Scraped data for {count} galleries')
			log.info('##############################')

		if config.bulk_url_scrape_movies:
			movies = self.client.find_movies(f={
				"is_missing": "front_image",
				"url": {
					"value": "",
					"modifier": "NOT_NULL"
				}
			})
			log.info(f'Found {len(movies)} movies with URLs')
			count = self.__scrape_movies_with_url(movies)
			log.info(f'Scraped data for {count} movies')

		return None

	def bulk_fragment_scrape(self):
		# Scrape Everything enabled in config

		for scraper_id, types in self.list_all_fragment_tags().items():
			
			if config.bulk_url_scrape_scenes:
				if types.get('SCENE'):
					scenes = self.client.find_scenes_with_tag({'name': types.get('SCENE')})
					self.__scrape_scenes_with_fragment(scenes, scraper_id)
			
			if config.bulk_url_scrape_galleries:
				if types.get('GALLERY'):
					galleries = self.client.find_galleries_with_tag( {'name': types.get('GALLERY') } )
					self.__scrape_galleries_with_fragment(galleries, scraper_id)

		return None



	def list_all_fragment_tags(self):
		fragment_tags = {}

		if config.fragment_scrape_scenes:
			for s in self.client.list_scene_scrapers('FRAGMENT'):
				if s in fragment_tags:
					fragment_tags[s]['SCENE'] = f'{self.scrape_with_prefix}s_{s}'
				else:
					fragment_tags[s] = {'SCENE': f'{self.scrape_with_prefix}s_{s}'}

		if config.fragment_scrape_galleries:
			for s in self.client.list_gallery_scrapers('FRAGMENT'):
				if s in fragment_tags:
					fragment_tags[s]['GALLERY'] = f'{self.scrape_with_prefix}g_{s}'
				else:
					fragment_tags[s] = {'GALLERY': f'{self.scrape_with_prefix}s_{s}'}

		if config.fragment_scrape_movies:
			for s in self.client.list_movie_scrapers('FRAGMENT'):
				if s in fragment_tags:
					fragment_tags[s]['MOVIE'] = f'{self.scrape_with_prefix}m_{s}'
				else:
					fragment_tags[s] = {'MOVIE': f'{self.scrape_with_prefix}s_{s}'}

		# might need to handle separately
		# if config.fragment_scrape_performers:
		# 	for s in self.client.list_performer_scrapers('FRAGMENT'):
		# 		fragment_tags[s] = f'{self.scrape_with_prefix}p_{s}'

		return fragment_tags

	def list_all_control_tags(self):
		control_tags = [ self.bulk_url_control_tag ]
		for supported_types in self.list_all_fragment_tags().values():
			control_tags.extend( supported_types.values() )
		return control_tags

	def get_control_tag_ids(self):
		control_ids = list()
		for tag_name in self.list_all_control_tags():
			tag_id = self.client.get_tag_id_from_name(tag_name)
			if tag_id == None:
				continue
			control_ids.append(tag_id)
		return control_ids


	def __scrape_with_fragment(self, scrape_type, scraper_id, items, __scrape, __update):
		last_request = -1
		if self.delay > 0:
			# Initialize last request with current time + delay time
			last_request = time.time() + self.delay

		# Number of scraped items
		count = 0
		total = len(items)

		log.info(f'Scraping {total} {scrape_type} with scraper: {scraper_id}')

		for i, item in enumerate(items):
			# Update status bar
			log.progress(i/total)

			self.wait()
			scraped_data = __scrape(item, scraper_id)

			if scraped_data is None:
				log.info(f"Scraper ({scraper_id}) did not return a result for {scrape_type} ({item.get('id')}) ")
				continue
			else:
				# No data has been found for this scene
				if not any(scraped_data.values()):
					log.info(f"Could not get data for {scrape_type} {item.get('id')}")
					continue

				success = __update(item, scraped_data)
				if not success:
					log.warning(f"Failed to scrape {scrape_type} {item.get('id')}")

				count += 1

		return count

	def __scrape_with_url(self, scrape_type, items, __scrape, __update):
		last_request = -1
		if self.delay > 0:
			# Initialize last request with current time + delay time
			last_request = time.time() + self.delay

		working_scrapers = set()
		missing_scrapers = set()

		# Number of items to scrape
		count = 0
		total = len(items)

		# Scrape if url not in missing_scrapers
		for i, item in enumerate(items):
			# Update status bar
			log.progress(i/total)

			if item.get('url') is None or item.get('url') == "":
				log.info(f"{scrape_type} {item.get('id')} is missing url")
				continue
			netloc = urlparse(item.get("url")).netloc
			if netloc in working_scrapers or netloc not in missing_scrapers:
				log.info(f"Scraping URL for {scrape_type} {item.get('id')}")
				self.wait()
				scraped_data = __scrape(item.get('url'))
				# If result is null, add url to missing_scrapers
				if scraped_data is None:
					log.warning(f"Missing scraper for {urlparse(item.get('url')).netloc}")
					missing_scrapers.add(netloc)
					continue
				else:
					working_scrapers.add(netloc)
				# No data has been found for this item
				if not any(scraped_data.values()):
					log.info(f"Could not get data for {scrape_type} {item.get('id')}")
					continue

				success = __update(item, scraped_data)
				if not success:
					log.warning(f"Failed to scrape {scrape_type} {item.get('id')}")

				log.debug(f"Scraped data for {scrape_type} {item.get('id')}")
				count += 1

		return count

	def __scrape_scenes_with_fragment(self, scenes, scraper_id):
		return self.__scrape_with_fragment(
			"scenes",
			scraper_id,
			scenes,
			self.client.run_scene_scraper,
			self.__update_scene_with_scrape_data
		)
	def __scrape_scenes_with_url(self, scenes):
		return self.__scrape_with_url(
			"scene",
			scenes,
			self.client.scrape_scene_url,
			self.__update_scene_with_scrape_data
		)
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
				elif config.create_missing_tags and tag.get('name') != "":
					# Capitalize each word
					tag_name = " ".join(x.capitalize() for x in tag.get('name').split(" "))
					log.info(f'Create missing tag: {tag_name}')
					tag_ids.append(self.client.create_tag({'name':tag_name}))
			if len(tag_ids) > 0:
				update_data['tag_ids'] = tag_ids

		if scraped_data.get('performers'):
			performer_ids = list()
			for performer in scraped_data.get('performers'):
				if performer.get('stored_id'):
					performer_ids.append(performer.get('stored_id'))
				elif config.create_missing_performers and performer.get('name') != "":
					# not expecting much from a scene scraper besides a name and url for a performer
					perf_in = {
						'name': " ".join(x.capitalize() for x in performer.get('name').split(" ")),
						'url':  performer.get('url')
					}
					log.info(f'Create missing performer: {perf_in.get("name")}')
					performer_ids.append(self.client.create_performer(perf_in))
			if len(performer_ids) > 0:
				update_data['performer_ids'] = performer_ids

		if scraped_data.get('studio'):
			log.debug(json.dumps(scraped_data.get('studio')))
			if dict_query(scraped_data, 'studio.stored_id'):
				update_data['studio_id'] = dict_query(scraped_data, 'studio.stored_id')
			elif config.create_missing_studios:
				studio = {}
				studio["name"] = " ".join(x.capitalize() for x in dict_query(scraped_data, 'studio.name').split(" "))
				studio["url"] = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(scene.get('url')))
				log.info(f'Creating missing studio {studio.get("name")}')
				update_data['studio_id'] = self.client.create_studio(studio)

		if scraped_data.get('movies'):
			movie_ids = list()
			for movie in scraped_data.get('movies'):
				if movie.get('stored_id'):
					movie_id = movie.get('stored_id')
					movie_ids.append( {'movie_id':movie_id, 'scene_index':None} )
				elif config.create_missing_movies and movie.get('name') != "":
					log.info(f'Create missing movie: "{movie.get("name")}"')
					
					movie_data = {
						'name': movie.get('name')
					}

					if movie.get('url'):
						movie_data['url'] = movie.get('url')

					if movie.get('synopsis'):
						movie_data['synopsis'] = movie.get('synopsis')
					if movie.get('date'):
						movie_data['date'] = movie.get('date')
					if movie.get('aliases'):
						movie_data['aliases'] = movie.get('aliases')

					try:
						movie_id = self.client.create_movie(movie_data)
						movie_ids.append( {'movie_id':movie_id, 'scene_index':None} )
					except Exception as e:
						log.error('update error')

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
			self.client.update_scene(update_data)
		except Exception as e:
			log.error('Error updating scene')
			log.error(json.dumps(update_data))
			log.error(str(e))

		return True

	def __scrape_galleries_with_fragment(self, galleries, scraper_id):
		return self.__scrape_with_fragment(
			"galleries",
			scraper_id,
			galleries,
			self.client.run_gallery_scraper,
			self.__update_gallery_with_scrape_data
		)
	def __scrape_galleries_with_url(self, galleries):
		return self.__scrape_with_url(
			"gallery",
			galleries,
			self.client.scrape_gallery_url,
			self.__update_gallery_with_scrape_data
		)
	def __update_gallery_with_scrape_data(self, gallery, scraped_data):

		# Expecting ScrapedGallery {
		# 		title
		# 		details
		# 		url
		# 		date
		# 		studio { ...scrapedSceneStudio }
		# 		tags [ ...ScrapedSceneTag ]
		# 		performers [ ...scrapedScenePerformer ]
		# }

		# Casting to GalleryUpdateInput {
		# 		id
		#     title
		# 		details
		# 		url
		# 		date
		#     rating
		# 		organized
		# 		scene_ids [ID!]
		# 		studio_id
		# 		tag_ids [ID!]
		# 		performer_ids [ID!]
		# }

		update_data = {
			'id': gallery.get('id')
		}

		common_attrabutes = [
			'title',
			'details',
			'url',
			'date'
		]
		for attr in common_attrabutes:
			if scraped_data.get(attr):
				update_data[attr] = scraped_data.get(attr)
		
		if scraped_data.get('tags'):
			tag_ids = list()
			for tag in scraped_data.get('tags'):
				if tag.get('stored_id'):
					tag_ids.append(tag.get('stored_id'))
				elif config.create_missing_tags and tag.get('name') != "":
					# Capitalize each word
					tag_name = " ".join(x.capitalize() for x in tag.get('name').split(" "))
					log.info(f'Create missing tag: {tag_name}')
					tag_ids.append(self.client.create_tag({'name':tag_name}))
			if len(tag_ids) > 0:
				update_data['tag_ids'] = tag_ids

		if scraped_data.get('performers'):
			performer_ids = list()
			for performer in scraped_data.get('performers'):
				if performer.get('stored_id'):
					performer_ids.append(performer.get('stored_id'))
				elif config.create_missing_performers and performer.get('name') != "":
					# not expecting much from a scene scraper besides a name and url for a performer
					perf_in = {
						'name': " ".join(x.capitalize() for x in performer.get('name').split(" ")),
						'url':  performer.get('url')
					}
					log.info(f'Create missing performer: {perf_in.get("name")}')
					performer_ids.append(self.client.create_performer(perf_in))
			if len(performer_ids) > 0:
				update_data['performer_ids'] = performer_ids

		if scraped_data.get('studio'):
			log.debug(json.dumps(scraped_data.get('studio')))
			if dict_query(scraped_data, 'studio.stored_id'):
				update_data['studio_id'] = dict_query(scraped_data, 'studio.stored_id')
			elif config.create_missing_studios:
				studio = {}
				studio["name"] = " ".join(x.capitalize() for x in dict_query(scraped_data, 'studio.name').split(" "))
				studio["url"] = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(gallery.get('url')))
				log.info(f'Creating missing studio {studio.get("name")}')
				update_data['studio_id'] = self.client.create_studio(studio)

		# Merge existing tags ignoring plugin control tags
		merged_tags = set()

		control_tag_ids = self.get_control_tag_ids()
		for tag in gallery.get('tags'):
			if tag.get('id') not in control_tag_ids:
				merged_tags.add(tag.get('id'))
		if update_data.get('tag_ids'):
			merged_tags.update(update_data.get('tag_ids'))

		update_data['tag_ids'] = list(merged_tags)


		# Update scene with scraped scene data
		try:
			self.client.update_gallery(update_data)
		except Exception as e:
			log.error('Error updating gallery')
			log.error(json.dumps(update_data))
			log.error(str(e))
			return False

		return True

	def __scrape_movies_with_url(self, movies):
		return self.__scrape_with_url(
			"movie",
			movies,
			self.client.scrape_movie_url,
			self.__update_movie_with_scrape_data
		)
	def __update_movie_with_scrape_data(self, movie, scraped_data):

		# Expecting  ScrapedMovie {
		# 		name
		# 		aliases
		# 		duration
		# 		date
		# 		rating
		# 		director
		# 		url
		# 		synopsis
		# 		studio {
		# 				...scrapedMovieStudio
		# 		}
		# 		front_image
		# 		back_image
		# }

		# Casting to MovieUpdateInput {
		# 		id
		#     name
		# 		aliases
		# 		duration
		# 		date
		# 		rating
		#     studio_id
		# 		director
		# 		url
		# 		synopsis
		# 		front_image
		# 		back_image
		# }

		update_data = {
			'id': movie.get('id')
		}

		common_attrabutes = [
			'name',
			'aliases',
			'duration',
			'date',
			'rating',
			'director',
			'url',
			'synopsis',
			'front_image',
			'back_image'
		]

		for attr in common_attrabutes:
			if scraped_data.get(attr):
				update_data[attr] = scraped_data.get(attr)

		if scraped_data.get('studio'):
			update_data['studio_id'] = scraped_data.get('studio').get('id')

		try:
			self.client.update_movie(update_data)
		except Exception as e:
			log.error('update movie error')
			return False

		return True


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
